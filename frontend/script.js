// AI generated

const server = "http://localhost:7000";
let nodes = [];

async function api(url, method = "GET", body = null) {
  try {
    const options = {
      method,
      headers: { "Content-Type": "application/json" },
    };
    if (body) options.body = JSON.stringify(body);
    return await (await fetch(url, options)).json();
  } catch (e) {
    return { error: e.message };
  }
}

function show(id, d) {
  document.getElementById(id).textContent = JSON.stringify(d, null, 2);
}

async function initCluster() {
  const count = parseInt(document.getElementById("node-count").value);
  const res = await api(server + "/init", "POST", { count });
  if (res.error) {
    document.getElementById("setup-err").textContent = res.error;
    return;
  }
  nodes = res.nodes;
  buildSelects();
  document.getElementById("setup").style.display = "none";
  document.getElementById("app").style.display = "block";
  setInterval(refresh, 1000);
  refresh();
}

function buildSelects() {
  ["kv-node", "lease-node", "dump-node"].forEach((id) => {
    const s = document.getElementById(id);
    s.innerHTML = "";
    nodes.forEach((n, i) =>
      s.add(new Option(n, "http://localhost:" + (8001 + i))),
    );
  });
}

async function refresh() {
  const r = await api(server + "/status");
  if (r.error) return;
  const d = document.getElementById("nodes");
  d.innerHTML = "";
  (r.nodes || []).forEach((n) => {
    let badgeClass = !n.alive
      ? "dead"
      : n.state === "Leader"
        ? "leader"
        : "follower";
    let badgeLabel = n.alive ? n.state : "DEAD";
    let info = n.alive
      ? `term=${n.term} leader=${n.leader || "?"} log=${n.logLength} commit=${n.commitIndex}`
      : "node is offline";
    d.innerHTML +=
      `<div class="node-row">` +
      `<span class="badge ${badgeClass}">${n.id} · ${badgeLabel}</span>` +
      `<span class="node-info">${info}</span>` +
      `<span class="node-actions">` +
      `${
        n.alive
          ? `<button class="danger" onclick="api(server+'/kill/${n.id}','POST').then(refresh)">Kill</button>`
          : `<button class="primary" onclick="api(server+'/restart/${n.id}','POST').then(refresh)">Restart</button>`
      }` +
      `<button class="subtle" onclick="api(server+'/wipe/${n.id}','POST').then(refresh)">Wipe</button>` +
      `</span>` +
      `</div>`;
  });
  dumpKV();
}

async function dumpKV() {
  show(
    "dump-out",
    await api(document.getElementById("dump-node").value + "/dump"),
  );
}

async function kvOp(op) {
  const b = document.getElementById("kv-node").value,
    k = document.getElementById("kv-key").value;
  if (!k) return show("kv-out", { error: "key required" });
  let r;
  if (op === "GET") r = await api(b + "/keys/" + k);
  else if (op === "PUT") {
    const body = {
      value: document.getElementById("kv-value").value,
    };
    const l = document.getElementById("kv-lease").value;
    if (l) body.leaseID = l;
    r = await api(b + "/keys/" + k, "PUT", body);
  } else if (op === "DELETE") r = await api(b + "/keys/" + k, "DELETE");
  else if (op === "CAS")
    r = await api(b + "/keys/" + k, "PATCH", {
      expected: document.getElementById("kv-expected").value,
      value: document.getElementById("kv-value").value,
    });
  show("kv-out", r);
}

async function leaseOp(op) {
  const b = document.getElementById("lease-node").value;
  let r;
  if (op === "grant") {
    const t = parseInt(document.getElementById("lease-ttl").value);
    if (!t) return show("lease-out", { error: "ttl required" });
    r = await api(b + "/lease", "POST", { ttl: t });
  } else if (op === "revoke") {
    const id = document.getElementById("lease-id").value;
    if (!id)
      return show("lease-out", {
        error: "lease id required",
      });
    r = await api(b + "/lease/" + id, "DELETE");
  } else if (op === "renew") {
    const id = document.getElementById("lease-id").value;
    if (!id)
      return show("lease-out", {
        error: "lease id required",
      });
    r = await api(b + "/lease/" + id + "/renew", "POST");
  } else if (op === "info") {
    const id = document.getElementById("lease-id").value;
    if (!id)
      return show("lease-out", {
        error: "lease id required",
      });
    r = await api(b + "/lease/" + id);
  } else if (op === "list") {
    r = await api(b + "/lease");
  }
  show("lease-out", r);
}
