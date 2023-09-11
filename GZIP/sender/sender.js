const rhea = require('rhea');
const zlib = require('zlib');

const connectionOptions = {
  host: '10.37.129.2',
  port: 61616,
  username: 'admin',
  password: 'admin',
};

const queueName = 'test/java';
const connection = rhea.connect(connectionOptions);

const intArray = generateRandomArray(100, 'int');
const floatArray = generateRandomArray(100, 'float');
const stringArray = generateRandomArray(100, 'string');

connection.on('connection_open', (context) => {
    console.log('Connected to the Artemis server');
  
    const sender = context.connection.open_sender(queueName);
  
    sender.on('sendable', (context) => {
     
  
      const message = new rhea.Message(); 
  
      const data = JSON.stringify({ intArray, floatArray, stringArray });
      zlib.gzip(jsonString)(data, (error, compressedData) => {
        if (error) {
          console.error('Error compressing data:', error);
          return;
        }
  
        message.body = { content: compressedData };
        context.sender.send(message);
        console.log('Sent zipped data');
      });
    });
    
    context.connection.on('disconnected', (context) => {
      console.error('Connection disconnected:', context.error);
    });
  });

process.on('SIGINT', () => {
  console.log('Disconnecting sender...');
  connection.close();
});

function generateRandomArray(size, type) {
  const array = [];
  for (let i = 0; i < size; i++) {
    if (type === 'int') {
      array.push(Math.floor(Math.random() * 1000));
    } else if (type === 'float') {
      array.push(Math.random() * 1000);
    } else if (type === 'string') {
      array.push(Math.random().toString(36).substring(7));
    }
  }
  return array;
}


