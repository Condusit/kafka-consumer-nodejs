const kafka = require('kafka-node');
const Consumer = kafka.Consumer;
const  client = new kafka.KafkaClient({kafkaHost : '13.59.230.114:9092'});
const offset = new kafka.Offset(client);
const  consumer = new Consumer(client,
        [{ groupId: 'krona-test',
            topic: 'raw-data-tech-autotrac-positions',
            commitOffsetsOnFirstJoin: true,
            outOfRangeOffset: 'earliest',
            fromOffset: 'latest', // default
            protocol: ['roundrobin'],
            encoding: 'utf8',
            keyEncoding: 'utf8',
            }],
        {
            autoCommit: false
        }
    );

consumer.on('message', function (message) {
    console.log(message);
});

consumer.on('error', function (err) {
    console.log('Error:',err);
});

consumer.on('offsetOutOfRange', function (topic) {
    console.log("------------- offsetOutOfRange ------------");
    topic.maxNum = 2;
    offset.fetch([topic], function (err, offsets) {
        var min = Math.min.apply(null, offsets[topic.topic][topic.partition]);
        consumer.setOffset(topic.topic, topic.partition, min);
    });
});

consumer.on('ready', function (err) {
    console.log('ready:',err);
});

consumer.on('connect', function (err) {
    console.log('connect:',err);
});


  

