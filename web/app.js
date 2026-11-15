// 接收页前端：1.5s 轮询 /api/overview，全部状态由接收存储驱动。
const POLL_MS = 1500;

const $ = (sel) => document.querySelector(sel);
const esc = (s) => String(s ?? "").replace(/[&<>"']/g, (c) => (
  { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]
));
const fmtBytes = (n) => {
  if (n === 0 || n == null) return "0 B";
  const units = ["B", "KB", "MB", "GB"];
  const i = Math.min(units.length - 1, Math.floor(Math.log(n) / Math.log(1024)));
  return `${(n / 1024 ** i).toFixed(i ? 1 : 0)} ${units[i]}`;
};
const badge = (text, cls) => `<span class="badge ${cls}">${esc(text)}</span>`;

async function api(path, options = {}) {
  const res = await fetch(path, {
    headers: { "Content-Type": "application/json" },
    ...options,
  });
  const body = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(body.error || `HTTP ${res.status}`);
  return body;
}

// ---------- 顶部控制 ----------
$("#btn-stop").addEventListener("click", async () => {
  await api("/api/receiver/stop", { method: "POST" });
  refresh();
});
$("#btn-resume").addEventListener("click", async () => {
  await api("/api/receiver/resume", { method: "POST" });
  refresh();
});

// ---------- 渲染 ----------
function renderReceiver(state, store) {
  const el = $("#receiver-state");
  el.textContent = state === "running" ? "接收器运行中" : "接收器已停止";
  el.className = `pill ${state}`;
  $("#btn-stop").disabled = state === "stopped";
  $("#btn-resume").disabled = state === "running";

  const saved = store.logical_bytes - store.physical_bytes;
  $("#store-stats").innerHTML = `
    <div class="stat"><div class="k">对象数（内容寻址去重后）</div>
      <div class="v">${store.objects}</div></div>
    <div class="stat"><div class="k">逻辑字节（按对象名累计）</div>
      <div class="v">${fmtBytes(store.logical_bytes)}</div></div>
    <div class="stat"><div class="k">物理占用（按 inode 去重）</div>
      <div class="v">${fmtBytes(store.physical_bytes)}</div></div>
    <div class="stat"><div class="k">硬链接/去重节省</div>
      <div class="v">${fmtBytes(Math.max(0, saved))}</div></div>`;
}

function renderManifests(manifests, currentEpoch) {
  return `<div class="manifests">` + manifests.map((m) => {
    const cls = m.state === "received" ? "received"
      : m.state === "stale" ? "stale"
      : m.state === "corrupt" ? "corrupt" : "writing";
    return `<div class="mcard">
      <div class="e">epoch ${m.epoch}${m.epoch === currentEpoch ? " ★当前" : ""}</div>
      <div>${badge(m.state, cls)}</div>
      <div class="note">${esc(m.source_mode)} · ${m.size} B</div>
      <div class="note">${esc(m.note || " ")}</div>
    </div>`;
  }).join("") + `</div>`;
}

function renderFileRow(item, batchState, currentEpoch) {
  const f = item.file;
  const versions = item.versions;
  const pub = item.publish;
  const current = versions.filter((v) => v.epoch === currentEpoch).slice(-1)[0];
  const state = current ? current.state : "writing";
  const stateBadge = badge(state, state);

  const storageBadge = current && current.storage_mode
    ? badge(current.storage_mode === "hardlink" ? "可靠引用(硬链接)" : "独立复制",
            current.storage_mode)
    : "";
  const pubBadge = pub ? badge(`已发布 epoch=${pub.epoch}`, "published") : "";

  const canPublish = batchState === "open" && state === "complete"
    && (!pub || pub.epoch !== currentEpoch);
  const publishBtn = canPublish
    ? `<button class="btn ok small" data-publish="${f.id}">
         ${pub ? "发布新版本" : "发布"}</button>`
    : `<button class="btn small" disabled>${pub ? "已发布" : "未判齐"}</button>`;
  const claimBtn = pub
    ? `<a class="btn small" href="/api/files/${f.id}/claim" target="_blank">下游领取</a>
       <button class="btn small" data-evidence="${f.id}">发布依据</button>`
    : "";

  const older = versions.filter((v) => v.epoch !== currentEpoch
    || (current && v.id !== current.id));
  const history = older.length
    ? `<details><summary class="note">历史版本（${older.length}）</summary>
       ${older.map((v) => `<div class="note">epoch ${v.epoch} ·
         <span class="mono">${esc(v.state)}</span>
         ${v.storage_mode ? "· " + esc(v.storage_mode) : ""}
         ${esc(v.note || "")}</div>`).join("")}</details>`
    : "";

  return `<tr>
    <td class="mono">${esc(f.relpath)}</td>
    <td>${stateBadge} ${storageBadge} ${pubBadge}</td>
    <td class="note">${esc(current ? current.note : "")}
      ${current && current.fallback_note ? `<br>${esc(current.fallback_note)}` : ""}
      ${history}</td>
    <td style="white-space:nowrap">${publishBtn} ${claimBtn}</td>
  </tr>`;
}

function renderBatch(block) {
  const b = block.batch;
  const batchBadge = badge(b.state === "withdrawn" ? "已撤回" : "开放中", b.state);
  const modeBadge = badge(b.source_mode, b.source_mode === "immutable" ? "hardlink" : "copy");
  return `<section class="batch">
    <div class="batch-head">
      <div>
        <div class="batch-title">${esc(b.batch_id)}
          <span class="note">${esc(b.title || "")}</span></div>
        <div class="note">当前 epoch=${b.current_epoch}
          ${b.withdrawn_at ? "· 撤回于 " + esc(b.withdrawn_at) + "：" + esc(b.withdraw_reason) : ""}
        </div>
      </div>
      <div class="badges">${batchBadge} ${modeBadge}
        <button class="btn danger small" data-withdraw="${esc(b.batch_id)}"
          ${b.state === "withdrawn" ? "disabled" : ""}>撤回目录</button>
      </div>
    </div>
    ${renderManifests(block.manifests, b.current_epoch)}
    <table>
      <thead><tr><th>文件</th><th>状态</th><th>观察结论 / 空间说明</th><th>操作</th></tr></thead>
      <tbody>${block.files.map((it) => renderFileRow(it, b.state, b.current_epoch)).join("")}</tbody>
    </table>
  </section>`;
}

function renderEvents(events) {
  $("#events").innerHTML = events.map((e) => `
    <div class="ev kind-${esc(e.kind)}">
      <span class="t">${esc(e.at)}</span>
      <span class="k">${esc(e.kind)}</span>
      <span>${esc(e.message)}</span>
    </div>`).join("") || `<div class="note">暂无事件</div>`;
}

async function refresh() {
  let data;
  try {
    data = await api("/api/overview");
  } catch (err) {
    $("#events").innerHTML = `<div class="note">轮询失败：${esc(err.message)}</div>`;
    return;
  }
  renderReceiver(data.receiver_state, data.store);
  $("#batches").innerHTML = data.batches.map(renderBatch).join("")
    || `<section class="panel note">投递区还没有批次（用 sender init 发布第一个目录通知）</section>`;
  renderEvents(data.events);
  bindActions(data);
}

function bindActions(data) {
  document.querySelectorAll("[data-publish]").forEach((btn) => {
    btn.onclick = async () => {
      try {
        const r = await api(`/api/files/${btn.dataset.publish}/publish`, { method: "POST" });
        alert(`发布成功。依据：${r.evidence.basis}`);
      } catch (err) {
        alert(err.message);
      }
      refresh();
    };
  });
  document.querySelectorAll("[data-withdraw]").forEach((btn) => {
    btn.onclick = async () => {
      const reason = prompt("撤回原因（下游将无法再领取该批次，历史记录保留）：", "目录有误，等待更正");
      if (reason === null) return;
      try {
        await api(`/api/batches/${encodeURIComponent(btn.dataset.withdraw)}/withdraw`, {
          method: "POST", body: JSON.stringify({ reason }),
        });
      } catch (err) {
        alert(err.message);
      }
      refresh();
    };
  });
  document.querySelectorAll("[data-evidence]").forEach((btn) => {
    btn.onclick = () => {
      const block = data.batches.flatMap((b) => b.files)
        .find((it) => String(it.file.id) === btn.dataset.evidence);
      const pub = block && block.publish;
      $("#evidence-title").textContent = pub
        ? `发布依据：${pub.relpath}（epoch ${pub.epoch}）` : "发布依据";
      $("#evidence-body").textContent = pub
        ? JSON.stringify(JSON.parse(pub.evidence), null, 2) : "无";
      $("#evidence-dialog").showModal();
    };
  });
}

refresh();
setInterval(refresh, POLL_MS);
