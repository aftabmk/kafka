const fs = require("fs");
const path = require("path");

class PartitionManager {
  constructor(baseDir = "data") {
    this.baseDir = baseDir;
  }

  ensurePartition(topic, partition) {
    const dir = path.join(this.baseDir, topic);
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });

    const file = path.join(dir, `partition-${partition}.log`);
    if (!fs.existsSync(file)) fs.writeFileSync(file, "");

    return file;
  }

  getOffset(file) {
    const data = fs.readFileSync(file, "utf8");
    if (!data) return 0;
    return data.split("\n").filter(Boolean).length;
  }

  append(topic, partition, value) {
    const file = this.ensurePartition(topic, partition);
    const offset = this.getOffset(file);

    fs.appendFileSync(file, value + "\n");
    return offset;
  }

  read(topic, partition, offset) {
    const file = this.ensurePartition(topic, partition);
    const lines = fs
      .readFileSync(file, "utf8")
      .split("\n")
      .filter(Boolean);

    return {
      records: lines.slice(offset),
      nextOffset: lines.length
    };
  }
}

module.exports = PartitionManager;
