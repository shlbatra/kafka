const { kafka } = require("./client");

async function init() {
  const admin = kafka.admin();
  console.log("Admin connecting...");
  admin.connect();
  console.log("Adming Connection Success...");

  console.log("Creating Topic [rider-updates1]");
  await admin.createTopics({
    topics: [
      {
        topic: "rider-updates1",
        numPartitions: 2,
      },
    ],
  });
  console.log("Topic Created Success [rider-updates1]");

  console.log("Disconnecting Admin..");
  await admin.disconnect();
}

init();