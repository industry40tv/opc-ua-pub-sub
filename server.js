const { OPCUAServer, DataType, resolveNodeId, AttributeIds } = require("node-opcua");
const { DataSetFieldContentMask, JsonDataSetMessageContentMask, JsonNetworkMessageContentMask, BrokerTransportQualityOfService, MyMqttJsonPubSubConnectionDataType, Transport, PublishedDataItemsDataType } = require("node-opcua-pubsub-expander");
const { installPubSub } = require("node-opcua-pubsub-server");
const { PubSubConfigurationDataType } = require("node-opcua-types");

/*_"some additionnal imports"*/

(async()=>{
    try {
        const server = new OPCUAServer({
            port: 26543
        });

        await server.initialize();

        /* Add a Temperature Sensor */
        const namespace = server.engine.addressSpace.getOwnNamespace();

        const sensor = namespace.addObject({
            browseName: "MySensor",
            organizedBy: server.engine.addressSpace.rootFolder.objects
        });

        const temperature = namespace.addVariable({
            browseName: "Temperature",
            nodeId: "s=Temperature",
            componentOf: sensor,
            dataType: "Double",
            value: { dataType: DataType.Double, value: 0 }
 
        });

        /*Note: The temperature variable nodeId has been set to `"ns=1;s=Temperature"`. */

        /* Simulate the Temperature */

        setInterval(() => {
            const value = 19 + 5 * Math.sin(Date.now() / 10000) + Math.random()*0.2;
            temperature.setValueFromSource({ dataType: "Double", value });
        }, 100);

        const configuration = getPubSubConfiguration();
        console.log(configuration.toString());
        //
        await installPubSub(server, {
        configuration,
        });

        

        /*_"enable pub-sub service"*/

        await server.start();
        console.log("server started at ", server.getEndpointUrl());
    } catch(err) {
        console.log(err);
        process.exit(1);
    }
})();

function getPubSubConfiguration()
{
  /*_"create the connection"*/
  const connection = createConnection();

  /*_"create the published dataset";*/
  const publishedDataSet = createPublishedDataSet();

  return new PubSubConfigurationDataType({
    connections: [connection],
    publishedDataSets: [publishedDataSet] });
}

function createConnection() {

    const mqttEndpoint = "mqtt:23.96.119.210:1883";

    /*_"create the writer group";*/

    /*_"create the dataset writer"*/

    const dataSetWriter = {
        dataSetFieldContentMask: DataSetFieldContentMask.None,
        dataSetName: "PublishedDataSet1",
        dataSetWriterId: 1,
        enabled: true,
        name: "dataSetWriter1",
        messageSettings: {
            dataSetMessageContentMask:
            JsonDataSetMessageContentMask.DataSetWriterId |
            JsonDataSetMessageContentMask.MetaDataVersion,
        },
        transportSettings: {
            queueName: "/opcuaovermqttdemo/temperature",
        },
    };

    const writerGroup = {
        dataSetWriters: [dataSetWriter],
        enabled: true,
        publishingInterval: 1000,
        name: "WriterGroup1",
        messageSettings: {
            networkMessageContentMask: JsonNetworkMessageContentMask.PublisherId,
        },
        transportSettings: {
            requestedDeliveryGuarantee: BrokerTransportQualityOfService.AtMostOnce,
        },
    };

    const connection = new MyMqttJsonPubSubConnectionDataType({
        enabled: true,
        name: "Connection1",
        transportProfileUri: Transport.MQTT_JSON,
        address: {
            url: mqttEndpoint,
        },
        writerGroups: [writerGroup],
        readerGroups: []
    });
    return connection;
}

/*_"create the dataset writer"*/



function createPublishedDataSet() {
    const publishedDataSet = {
        name: "PublishedDataSet1",
        dataSetMetaData: {
            fields: [
                {
                    name: "SensorTemperature",
                    builtInType: DataType.Double,
                    dataType: resolveNodeId("Double"),
                },
            ],
        },
        dataSetSource: new PublishedDataItemsDataType({
            publishedData: [
                {
                    attributeId: AttributeIds.Value,
                    samplingIntervalHint: 1000,
                    publishedVariable: `ns=1;s=Temperature`,
                },
            ],
        }),
    };
    return publishedDataSet;
}

/*_"constructing the configuration parameters"*/