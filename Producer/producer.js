const net = require("net");

const BROKER_PORT = 9092;
const BROKER_HOST = "127.0.0.1";

const TOPIC = "receipt";

// Optional: use a key to assign partition
function getPartition(key, numPartitions = 2) {
  // simple hash
  let hash = 0;
  for (let i = 0; i < key.length; i++) {
    hash = (hash << 5) - hash + key.charCodeAt(i);
    hash |= 0;
  }
  return Math.abs(hash) % numPartitions;
}

// Messages to send
const messages = [
  { key: "receipt1", value: "receipt_created" },
  { key: "receipt2", value: "receipt_paid" },
  { key: "receipt1", value: "receipt_generated" }
];

let requestCounter = 1;
let pendingAcks = messages.length;

const socket = net.createConnection(BROKER_PORT, BROKER_HOST, () => {
  console.log("Producer connected to broker");

  messages.forEach(msg => {
    send(msg);
  });
});

/* ---------- SEND MESSAGE ---------- */
function send({ key, value }) {
  const partition = getPartition(key); // optional key-based partition

  const msg = {
    type: "PRODUCE",
    requestId: requestCounter++,
    topic: TOPIC,
    partition,
    value
  };

  const payload = JSON.stringify(msg);
  const len = Buffer.alloc(4);
  len.writeInt32BE(Buffer.byteLength(payload));

  socket.write(Buffer.concat([len, Buffer.from(payload)]));
}

/* ---------- RECEIVE ACK ---------- */
let buffer = Buffer.alloc(0);

socket.on("data", chunk => {
  buffer = Buffer.concat([buffer, chunk]);

  while (buffer.length >= 4) {
    const len = buffer.readInt32BE(0);
    if (buffer.length < len + 4) break;

    const payload = buffer.slice(4, 4 + len);
    buffer = buffer.slice(4 + len);

    const res = JSON.parse(payload.toString());
    console.log("ACK:", res);

    pendingAcks--;

    // Close producer safely when all messages are ACKed
    if (pendingAcks === 0) {
      console.log("All messages acknowledged, closing producer.");
      socket.end();
    }
  }
});

/* ---------- SOCKET EVENTS ---------- */
socket.on("close", () => console.log("Producer connection closed"));
socket.on("error", err => console.error("Producer error:", err.message));
