const rhea = require('rhea');
const zlib = require('zlib');
const fs = require('fs');

const connectionOptions = {
  host: '10.37.129.2',
  port: 61616,
  username: 'admin',
  password: 'admin',
};

const queueName = 'test/java';
const connection = rhea.connect(connectionOptions);

connection.on('connection_open', (context) => {
  console.log('Connected to the Artemis server');
  context.connection.open_receiver(queueName);
});

connection.on('message', async (context) => {
  const message = context.message;

  const compressedData = message.body.content;
  const compressedSize = compressedData.length; 

  console.log(`Received compressed data size: ${compressedSize} bytes`);

  // Unzip the gzipped data
  zlib.unzip(compressedData, (error, uncompressedData) => {
    if (error) {
      console.error('Error unzipping data:', error);
      return;
    }

    const jsonString = uncompressedData.toString('utf8');
    try {
      const jsonBody = JSON.parse(jsonString);
      console.log('Received JSON message:', jsonBody);

      // Save the JSON 
      const filename = `received_message.json`;
      fs.writeFileSync(filename, JSON.stringify(jsonBody, null, 2));
      console.log(`Saved JSON data to ${filename}`);
    } catch (error) {
      console.error('Error parsing JSON:', error);
    }

    // Acknowledge the received message
    context.delivery.accept();
  });
});

connection.on('disconnected', (context) => {
  console.error('Receiver disconnected:', context.error);
});

process.on('SIGINT', () => {
  console.log('Disconnecting receiver...');
  connection.close();
});
