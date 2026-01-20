// broker.js
const net = require("net");
const PartitionManager = require("./partitionManager");

const PORT = 9092;
const pm = new PartitionManager();

const groupState = {};

const server = net.createServer(socket => {
  let buffer = Buffer.alloc(0);

  socket.on("data", chunk => {
    buffer = Buffer.concat([buffer, chunk]);

    while (buffer.length >= 4) {
      const len = buffer.readInt32BE(0);
      if (buffer.length < len + 4) break;

      const payload = buffer.slice(4, 4 + len);
      buffer = buffer.slice(4 + len);

      const msg = JSON.parse(payload.toString());

      switch (msg.type) {
        case "PRODUCE":
          handleProduce(socket, msg);
          break;

        case "JOIN_GROUP":
          handleJoinGroup(socket, msg);
          break;

        case "FETCH":
          handleFetch(socket, msg);
          break;

        case "COMMIT_OFFSET":
          handleCommit(socket, msg);
          break;
      }
    }
  });

  socket.on("error", () => {});
});

/* ---------- PRODUCE ---------- */

function handleProduce(socket, msg) {
  const offset = pm.append(
    msg.topic,
    msg.partition,
    msg.value
  );

  send(socket, {
    requestId: msg.requestId,
    status: "OK",
    offset
  });
}

/* ---------- JOIN GROUP ---------- */

function handleJoinGroup(socket, msg) {
  const { groupId, consumerId, topic, partitions } = msg;

  if (!groupState[groupId]) {
    groupState[groupId] = {
      topic,
      consumers: [],
      assignments: {},
      offsets: {}
    };
  }

  const group = groupState[groupId];

  if (!group.consumers.includes(consumerId)) {
    group.consumers.push(consumerId);
  }

  // simple static assignment: consumer index == partition
  group.assignments = {};
  group.consumers.forEach((cid, idx) => {
    group.assignments[cid] = [idx];
    if (group.offsets[idx] === undefined) {
      group.offsets[idx] = 0;
    }
  });

  send(socket, {
    status: "OK",
    partitions: group.assignments[consumerId] || []
  });
}

/* ---------- FETCH ---------- */

function handleFetch(socket, msg) {
  const { groupId, consumerId, topic, partition } = msg;
  const group = groupState[groupId];

  if (!group) {
    send(socket, { records: [], nextOffset: 0 });
    return;
  }

  // enforce assignment
  const assigned = group.assignments[consumerId] || [];
  if (!assigned.includes(partition)) {
    send(socket, { records: [], nextOffset: group.offsets[partition] || 0 });
    return;
  }

  const offset = group.offsets[partition] || 0;
  const res = pm.read(topic, partition, offset);

  send(socket, res);
}

/* ---------- COMMIT OFFSET ---------- */

function handleCommit(socket, msg) {
  const { groupId, partition, offset } = msg;

  const group = groupState[groupId];
  if (!group) return;

  group.offsets[partition] = offset;

  send(socket, { status: "OK" });
}

/* ---------- SEND FRAME ---------- */

function send(socket, obj) {
  const payload = JSON.stringify(obj);
  const len = Buffer.alloc(4);
  len.writeInt32BE(Buffer.byteLength(payload));
  socket.write(Buffer.concat([len, Buffer.from(payload)]));
}

/* ---------- START ---------- */

server.listen(PORT, () => {
  console.log("Broker listening on", PORT);
});
