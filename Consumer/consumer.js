const net = require("net");

const BROKER_PORT = 9092;
const BROKER_HOST = "127.0.0.1";

const GROUP_ID = "payment-service";      // consumer group
const CONSUMER_ID = "c1";               // unique consumer id
const TOPIC = "receipt";

let buffer = Buffer.alloc(0);
let assignedPartitions = [];
let offsets = {};   // partition => current offset

const socket = net.createConnection(BROKER_PORT, BROKER_HOST, () => {
  console.log("Consumer connected to broker");

  // Step 1: join group
  joinGroup();
});

/* ---------- JOIN GROUP ---------- */
function joinGroup() {
  const msg = {
    type: "JOIN_GROUP",
    groupId: GROUP_ID,
    consumerId: CONSUMER_ID,
    topic: TOPIC
  };
  send(msg);
}

/* ---------- FETCH LOOP ---------- */
function fetch() {
  if (assignedPartitions.length === 0) return;

  assignedPartitions.forEach(partition => {
    const msg = {
      type: "FETCH",
      groupId: GROUP_ID,
      consumerId: CONSUMER_ID,
      topic: TOPIC,
      partition,
      offset: offsets[partition] || 0
    };
    send(msg);
  });
}

/* ---------- HANDLE RESPONSE ---------- */
socket.on("data", chunk => {
  buffer = Buffer.concat([buffer, chunk]);

  while (buffer.length >= 4) {
    const len = buffer.readInt32BE(0);
    if (buffer.length < len + 4) break;

    const payload = buffer.slice(4, 4 + len);
    buffer = buffer.slice(4 + len);

    const res = JSON.parse(payload.toString());
    handleResponse(res);
  }
});

/* ---------- RESPONSE LOGIC ---------- */
function handleResponse(res) {
  // 1. JOIN_GROUP response
  if (res.partitions) {
    assignedPartitions = res.partitions;
    console.log("Assigned partitions:", assignedPartitions);

    // initialize offsets
    assignedPartitions.forEach(p => (offsets[p] = 0));

    // start polling
    fetch();
    return;
  }

  // 2. FETCH response
  if (res.records) {
    res.records.forEach((msg, idx) => {
      console.log("Consumed:", msg);
      // update local offset
      const part = assignedPartitions[0]; // simple: single partition per consumer
      offsets[part] = (offsets[part] || 0) + 1;

      // commit offset to broker
      commitOffset(part, offsets[part]);
    });

    // poll again
    setImmediate(fetch);
  }

  // 3. COMMIT_OFFSET response
  if (res.status === "OK" && !res.records) {
    // commit acknowledged
  }
}

/* ---------- COMMIT OFFSET ---------- */
function commitOffset(partition, offset) {
  const msg = {
    type: "COMMIT_OFFSET",
    groupId: GROUP_ID,
    partition,
    offset
  };
  send(msg);
}

/* ---------- SEND FRAME ---------- */
function send(obj) {
  const payload = JSON.stringify(obj);
  const len = Buffer.alloc(4);
  len.writeInt32BE(Buffer.byteLength(payload));
  socket.write(Buffer.concat([len, Buffer.from(payload)]));
}

/* ---------- SOCKET EVENTS ---------- */
socket.on("close", () => console.log("Consumer disconnected"));
socket.on("error", err => console.error("Consumer error:", err.message));
