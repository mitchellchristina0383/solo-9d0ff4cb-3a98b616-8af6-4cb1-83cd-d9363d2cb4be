"""HTTP 入口网关（FastAPI + httpx）。

职责：
  * 仅转发 GET 请求到上游，按轮询（round-robin）选择「健康且未排空」的节点；
  * 周期性主动探测上游，探测结论只影响健康标志，绝不改动人工排空标志；
  * 提供排空 / 恢复 / 状态管理 API；所有节点都不可接流时返回 503；
  * 排空期间新请求不再进入该节点，已在途的请求继续等待原响应返回；
  * 每次 drain 都是一轮独立指令；resume 或更新的一轮 drain 会使旧轮次立即失效，
    旧等待者必须报告 revoked（409），绝不借用别的轮次结果报告 drained=true；
  * 只有「本轮仍有效 + draining 仍为真 + inflight 归零」才报告可下线；
  * 排空等待超时也先核对本轮有效性：失效则报撤销，仍有效才报超时；
  * 超时 / 连接错误 / 5xx 不虚报成功，并同步刷新健康状态，在途计数靠 try/finally 保证准确。
"""
from __future__ import annotations

import asyncio
import os
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Optional

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response

# ----------------------------- 配置 -----------------------------

UPSTREAMS_RAW = os.environ.get(
    "UPSTREAMS",
    "node1=http://upstream1:8000,node2=http://upstream2:8000",
)
PROBE_INTERVAL = float(os.environ.get("PROBE_INTERVAL", "2.0"))
PROBE_TIMEOUT = float(os.environ.get("PROBE_TIMEOUT", "1.5"))
REQUEST_TIMEOUT = float(os.environ.get("REQUEST_TIMEOUT", "15.0"))
DRAIN_DEFAULT_TIMEOUT = float(os.environ.get("DRAIN_DEFAULT_TIMEOUT", "30.0"))

# 透传响应时需要剥离的逐跳（hop-by-hop）头
_HOP_BY_HOP = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "transfer-encoding",
    "upgrade",
    "content-length",
    "content-encoding",
}


def _parse_upstreams(raw: str) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    for item in raw.split(","):
        item = item.strip()
        if not item:
            continue
        name, _, url = item.partition("=")
        if not _:
            raise ValueError(f"非法 UPSTREAMS 条目: {item!r}，应为 name=url")
        out.append((name.strip(), url.strip().rstrip("/")))
    if not out:
        raise ValueError("至少需要配置一个上游节点")
    return out


# ----------------------------- 节点状态 -----------------------------

@dataclass
class NodeState:
    name: str
    base_url: str
    # 健康标志：只由探测任务（以及请求失败时的降级）写入
    healthy: bool = False
    # 排空标志：只由人工 drain/resume 写入，探测永不触碰
    draining: bool = False
    inflight: int = 0
    rr_hits: int = 0
    success_count: int = 0
    failure_count: int = 0
    last_probe_at: Optional[float] = None
    last_probe_ok: Optional[bool] = None
    last_probe_detail: str = "尚未探测"
    last_change_at: float = field(default_factory=time.time)
    # 排空轮次：每次 drain 开启新一轮、每次 resume 作废当前轮，均递增。
    # 等待者只认自己启动时捕获的轮次，绝不用别的轮次的结果报告成功。
    drain_epoch: int = 0

    def can_serve(self) -> bool:
        return self.healthy and not self.draining

    def snapshot(self) -> dict:
        return {
            "name": self.name,
            "base_url": self.base_url,
            "healthy": self.healthy,
            "draining": self.draining,
            "drain_epoch": self.drain_epoch,
            "eligible": self.can_serve(),
            "inflight": self.inflight,
            "rr_hits": self.rr_hits,
            "success_count": self.success_count,
            "failure_count": self.failure_count,
            "last_probe_at": self.last_probe_at,
            "last_probe_ok": self.last_probe_ok,
            "last_probe_detail": self.last_probe_detail,
        }


@dataclass
class DrainResult:
    """单轮排空的结论：drained / timeout / revoked，后两者都不得作为可下线回执。"""

    outcome: str
    inflight: int
    epoch: int
    detail: str

    @property
    def drained(self) -> bool:
        return self.outcome == "drained"

    @property
    def revoked(self) -> bool:
        return self.outcome == "revoked"


class Gateway:
    def __init__(self, nodes: list[NodeState]) -> None:
        self.nodes = nodes
        self._by_name = {n.name: n for n in nodes}
        # 一把条件变量保护所有节点状态；请求结束 / 排空等待都在其上通知
        self.cond = asyncio.Condition()
        self._rr = 0
        self.client: Optional[httpx.AsyncClient] = None
        self._probe_task: Optional[asyncio.Task] = None

    def get(self, name: str) -> Optional[NodeState]:
        return self._by_name.get(name)

    async def pick_node(self) -> Optional[NodeState]:
        """轮询选取健康且未排空的节点，并立即登记在途计数。"""
        async with self.cond:
            n = len(self.nodes)
            for offset in range(n):
                idx = (self._rr + offset) % n
                node = self.nodes[idx]
                if node.can_serve():
                    self._rr = (idx + 1) % n
                    node.inflight += 1
                    node.rr_hits += 1
                    return node
            return None

    async def release_node(self, node: NodeState, ok: bool) -> None:
        async with self.cond:
            node.inflight -= 1
            if ok:
                node.success_count += 1
            else:
                node.failure_count += 1
            # 在途归零可能让 drain 等待者满足条件
            self.cond.notify_all()

    async def mark_unhealthy(self, node: NodeState, detail: str) -> None:
        """请求失败时的即时降级；只改健康标志，不动 draining。"""
        async with self.cond:
            node.healthy = False
            node.last_probe_ok = False
            node.last_probe_detail = f"请求失败降级: {detail}"
            node.last_probe_at = time.time()
            node.last_change_at = time.time()

    async def probe_one(self, node: NodeState) -> None:
        """对单个节点做主动探测。成功只置 healthy=True，绝不解除 draining。"""
        assert self.client is not None
        try:
            resp = await self.client.get(
                f"{node.base_url}/", timeout=PROBE_TIMEOUT
            )
            ok = resp.status_code < 500
            detail = f"HTTP {resp.status_code}"
        except httpx.TimeoutException as exc:
            ok, detail = False, f"探测超时: {type(exc).__name__}"
        except httpx.RequestError as exc:
            ok, detail = False, f"探测连接错误: {type(exc).__name__}: {str(exc)[:100]}"

        async with self.cond:
            node.healthy = ok
            # 注意：这里刻意不修改 node.draining
            node.last_probe_at = time.time()
            node.last_probe_ok = ok
            node.last_probe_detail = detail
            node.last_change_at = time.time()

    async def probe_all(self) -> None:
        await asyncio.gather(*(self.probe_one(n) for n in self.nodes))

    async def _probe_loop(self) -> None:
        while True:
            try:
                await self.probe_all()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # 探测循环本身不能崩
                print(f"[probe-loop] unexpected error: {exc!r}")
            await asyncio.sleep(PROBE_INTERVAL)

    @staticmethod
    def _drain_revoked(node: NodeState, epoch: int) -> bool:
        """本轮排空是否已被撤销：resume 或更新的一轮 drain 都会使其失效。"""
        return node.drain_epoch != epoch or not node.draining

    async def drain(self, node: NodeState, timeout: Optional[float]) -> DrainResult:
        """开启新一轮排空并等待在途清零。

        只认本次调用捕获的轮次：
          * resume / 后发起的 drain 会递增 epoch 并唤醒等待者，本轮立即作废；
          * 作废后无论当前在途是否为 0，都只能报告 revoked，不能给出可下线回执；
          * 超时同样先核对轮次有效性：已作废报 revoked，仍有效才报 timeout。
        """
        loop = asyncio.get_running_loop()
        deadline = None if timeout is None else loop.time() + max(timeout, 0.0)
        async with self.cond:
            # 每一轮都是独立指令，旧轮次从此不再有资格报告任何结果
            node.drain_epoch += 1
            epoch = node.drain_epoch
            node.draining = True
            node.last_change_at = time.time()
            # 唤醒上一轮等待者，让它们立即以 revoked 收尾，而不是等到本轮结束
            self.cond.notify_all()
            while True:
                if self._drain_revoked(node, epoch):
                    return DrainResult(
                        "revoked", node.inflight, epoch,
                        "本轮排空已被恢复或更新的一轮排空撤销，不得据此下线",
                    )
                if node.inflight == 0:
                    return DrainResult(
                        "drained", 0, epoch, "在途请求已全部结束，本轮排空完成"
                    )
                if deadline is None:
                    await self.cond.wait()
                    continue
                remaining = deadline - loop.time()
                if remaining <= 0:
                    # 超时结论也必须反映本轮指令是否仍有效
                    if self._drain_revoked(node, epoch):
                        return DrainResult(
                            "revoked", node.inflight, epoch,
                            "等待超时，但本轮排空此前已被恢复或更新的一轮排空撤销",
                        )
                    return DrainResult(
                        "timeout", node.inflight, epoch,
                        "等待在途请求结束超时，节点仍处于本轮排空中",
                    )
                # asyncio.Condition.wait 不支持 timeout，用 wait_for 限时等待
                try:
                    await asyncio.wait_for(self.cond.wait(), timeout=remaining)
                except asyncio.TimeoutError:
                    pass  # 回到循环重新核对轮次、在途数与剩余时间

    async def resume(self, node: NodeState) -> None:
        """恢复接流：作废当前排空轮次并唤醒旧等待者，再经真实探测决定能否接流。"""
        async with self.cond:
            # 递增 epoch 使仍在等待的旧排空立即失效，它们只能报告 revoked；
            # 同时清排空并保守置为不健康，探测结论出来前一律不接流，杜绝状态窗口。
            node.drain_epoch += 1
            node.draining = False
            node.healthy = False
            node.last_probe_ok = None
            node.last_probe_detail = "恢复接流，等待首次探测确认"
            node.last_change_at = time.time()
            # 唤醒旧排空等待者，让它们尽快带着撤销结论返回
            self.cond.notify_all()
        await self.probe_one(node)


# ----------------------------- 应用生命周期 -----------------------------

gateway = Gateway([NodeState(name, url) for name, url in _parse_upstreams(UPSTREAMS_RAW)])


@asynccontextmanager
async def lifespan(app: FastAPI):
    async with httpx.AsyncClient() as client:
        gateway.client = client
        # 启动先做一轮探测，避免把流量打到未知状态的节点
        await gateway.probe_all()
        gateway._probe_task = asyncio.create_task(gateway._probe_loop())
        try:
            yield
        finally:
            if gateway._probe_task:
                gateway._probe_task.cancel()
                try:
                    await gateway._probe_task
                except asyncio.CancelledError:
                    pass


app = FastAPI(
    title="http-entry-gateway",
    version="1.0.0",
    description=(
        "仅转发 GET 的入口网关：轮询健康且未排空的上游，支持排空/恢复/状态管理，"
        "无可接流节点时返回 503。"
    ),
    lifespan=lifespan,
)


# ----------------------------- 转发接口 -----------------------------

@app.get("/", tags=["meta"])
async def root() -> dict:
    return {
        "service": "http-entry-gateway",
        "proxy": "GET /proxy/{path} 仅支持 GET",
        "admin": [
            "GET  /admin/status",
            "POST /admin/nodes/{name}/drain?timeout=",
            "POST /admin/nodes/{name}/resume",
            "POST /admin/probe",
        ],
    }


@app.get("/proxy/{path:path}", tags=["proxy"])
async def proxy(path: str, request: Request) -> Response:
    node = await gateway.pick_node()
    if node is None:
        return JSONResponse(
            status_code=503,
            content={
                "error": "no_eligible_upstream",
                "detail": "当前没有健康且未排空的上游节点可接流",
            },
        )

    target_url = f"{node.base_url}/{path}"
    ok = False
    try:
        try:
            upstream_resp = await gateway.client.request(
                "GET",
                target_url,
                params=request.query_params,
                timeout=REQUEST_TIMEOUT,
            )
        except httpx.TimeoutException as exc:
            await gateway.mark_unhealthy(node, f"请求超时 {type(exc).__name__}")
            return JSONResponse(
                status_code=504,
                content={"error": "upstream_timeout", "node": node.name},
            )
        except httpx.RequestError as exc:
            await gateway.mark_unhealthy(
                node, f"{type(exc).__name__}: {str(exc)[:100]}"
            )
            return JSONResponse(
                status_code=502,
                content={"error": "upstream_unreachable", "node": node.name},
            )

        # 5xx 视为上游故障：如实透传状态码并降级，绝不虚报成功
        if upstream_resp.status_code >= 500:
            await gateway.mark_unhealthy(
                node, f"上游返回 HTTP {upstream_resp.status_code}"
            )

        ok = upstream_resp.status_code < 500
        headers = {
            k: v
            for k, v in upstream_resp.headers.items()
            if k.lower() not in _HOP_BY_HOP
        }
        headers["X-Gateway"] = "http-entry-gateway"
        headers["X-Upstream-Node"] = node.name
        return Response(
            content=upstream_resp.content,
            status_code=upstream_resp.status_code,
            headers=headers,
            media_type=upstream_resp.headers.get("content-type"),
        )
    finally:
        # 无论成功、失败还是取消，在途计数都必须准确归还
        await gateway.release_node(node, ok)


# 非 GET 方法访问转发路径：FastAPI 自动返回 405 Method Not Allowed


# ----------------------------- 管理接口 -----------------------------

@app.get("/admin/status", tags=["admin"])
async def admin_status() -> dict:
    async with gateway.cond:
        return {
            "probe_interval": PROBE_INTERVAL,
            "request_timeout": REQUEST_TIMEOUT,
            "nodes": [n.snapshot() for n in gateway.nodes],
            "eligible_nodes": [n.name for n in gateway.nodes if n.can_serve()],
        }


@app.post("/admin/nodes/{name}/drain", tags=["admin"])
async def admin_drain(name: str, timeout: Optional[float] = DRAIN_DEFAULT_TIMEOUT):
    """排空节点：立即停止分配新请求，并阻塞等待在途请求全部结束。

    * 200 drained=true：本轮仍有效且在途已归零，可作为下线回执；
    * 202 drained=false：本轮仍有效但等待超时，节点继续保持排空中；
    * 409 revoked=true：本轮已被 resume 或更新的一轮 drain 撤销，
      即使此刻在途为 0 也绝不报告可下线。
    """
    node = gateway.get(name)
    if node is None:
        return JSONResponse(status_code=404, content={"error": "unknown_node", "name": name})

    result = await gateway.drain(node, timeout)
    # 结果与当前状态在同一把锁下取快照，避免报告与真实状态脱节
    async with gateway.cond:
        snap = node.snapshot()
    # 任何回执（含成功与超时）都只在「本轮仍有效且节点仍在排空中」时签发：
    # 若返回前恰好被 resume / 新一轮 drain 撤销，则降级为 revoked，
    # 绝不签发失效的可下线回执或过时的超时结论。
    if result.outcome != "revoked" and (
        snap["drain_epoch"] != result.epoch or not snap["draining"]
    ):
        result = DrainResult(
            "revoked", snap["inflight"], result.epoch,
            "结论签发前本轮已被恢复或更新的一轮排空撤销，不得据此下线",
        )
    if result.outcome == "revoked":
        return JSONResponse(
            status_code=409,
            content={
                "node": name,
                "draining": snap["draining"],
                "drained": False,
                "revoked": True,
                "outcome": result.outcome,
                "inflight": result.inflight,
                "drain_epoch": result.epoch,
                "current_drain_epoch": snap["drain_epoch"],
                "detail": result.detail,
            },
        )
    drained = result.outcome == "drained"
    body = {
        "node": name,
        "draining": snap["draining"],
        "drained": drained,
        "revoked": False,
        "outcome": result.outcome,
        "inflight": result.inflight,
        "drain_epoch": result.epoch,
        "current_drain_epoch": snap["drain_epoch"],
        "detail": result.detail,
    }
    return JSONResponse(status_code=200 if drained else 202, content=body)


@app.post("/admin/nodes/{name}/resume", tags=["admin"])
async def admin_resume(name: str):
    node = gateway.get(name)
    if node is None:
        return JSONResponse(status_code=404, content={"error": "unknown_node", "name": name})

    await gateway.resume(node)
    snap = node.snapshot()
    return {
        "node": name,
        "draining": False,
        "drain_epoch": snap["drain_epoch"],
        "healthy": snap["healthy"],
        "eligible": snap["eligible"],
        "last_probe_detail": snap["last_probe_detail"],
        "detail": "已解除排空并作废旧排空轮次；是否可接流以健康探测结论为准",
    }


@app.post("/admin/probe", tags=["admin"])
async def admin_probe() -> dict:
    """手动触发一轮探测（探测不会改变任何节点的排空标志）。"""
    await gateway.probe_all()
    async with gateway.cond:
        return {"nodes": [n.snapshot() for n in gateway.nodes]}
