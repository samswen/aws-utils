'use strict';

const AWS = require('aws-sdk');
const stream = require('stream');
const { createReadStream, createWriteStream } = require('fs');
const { listFolderRecursive } = require('@samwen/fs-utils');

module.exports = {
    setup,
    create_cname,
    delete_cname,
    publish_notification,
    file_exists,
    upload_file,
    upload_folder,
    download_file,
    register_task_definition,
    deregister_task_definition,
    create_ecs_service,
    stop_ecs_service,
    delete_ecs_service,
    get_queue_size_approximation,
    send_sqs_message,
    get_sqs_messages,
    delete_sqs_messages,
    delete_sqs_message,
    get_signed_url,
    delete_folder,
    delete_file,
};

// eslint-disable-next-line prefer-const
let logger = null;

let aws_account = null;
let aws_region = null;
let aws_credentials = null;
let aws_route53_info = null;
let aws_queue_urls = null;
let aws_vpc = null;
let aws_security_groups = null;
let aws_private_subnets = null;
let aws_public_subnets = null;

function setup(config, arg_logger) {
    if (!config) {
        throw new Error('call setup without config');
    }
    if (!config.get('aws_account') || !config.get('aws_credentials')) {
        throw new Error('config missing minima required aws_account and/or aws_credentials');
    }
    if (!arg_logger) {
        logger = console;
    } else {
        logger = arg_logger;
    }
    aws_account = config.get('aws_account');
    aws_credentials = config.get('aws_credentials');
    aws_region =  config.get('aws_region');
    if (!aws_region) {
        aws_region = 'us-east-1';
    }
    aws_route53_info = config.get('aws_route53_info');
    aws_queue_urls = config.get('aws_queue_urls');
    aws_security_groups = config.get('aws_security_groups');
    // eslint-disable-next-line no-unused-vars
    aws_vpc = config.get('aws_vpc');
    // eslint-disable-next-line no-unused-vars
    aws_public_subnets = config.get('aws_public_subnets');
    aws_private_subnets = config.get('aws_private_subnets');
    aws_credentials.region = aws_region;
    AWS.config.update(aws_credentials);
}

function create_cname(name, route53_info) {
    if (!route53_info) {
        route53_info = aws_route53_info;
    }
    return new Promise((resolve) => {
        const route53 = new AWS.Route53();
        const params = {
            HostedZoneId: route53_info.hostedZoneId,
            ChangeBatch: { Changes: [ { Action: 'CREATE',
                ResourceRecordSet: {
                    Name: name + '.' + route53_info.name,
                    Type: 'CNAME',
                    TTL: 300,
                    ResourceRecords: [{ Value: route53_info.value }]
                }
            }]}
        };
        route53.changeResourceRecordSets(params, function(err, data) {
            if (err) {
                if (err.message.indexOf('it already exists') > 0) {
                    resolve(true);
                    return;
                }
                logger.error(err);
                resolve(false);
                return;
            }
            resolve(data);
        });
    });
}

function delete_cname(name, route53_info) {
    if (!route53_info) {
        route53_info = aws_route53_info;
    }
    return new Promise((resolve) => {
        const route53 = new AWS.Route53();
        const params = {
            HostedZoneId: route53_info.hostedZoneId,
            ChangeBatch: { Changes: [ { Action: 'DELETE',
                ResourceRecordSet: {
                    Name: name + '.' + route53_info.name,
                    Type: 'CNAME',
                    TTL: 300,
                    ResourceRecords: [{ Value: route53_info.value }]
                }
            }]}
        };
        route53.changeResourceRecordSets(params, function(err, data) {
            if (err) {
                if (err.message.indexOf('it was not found') > 0) {
                    resolve(true);
                    return;
                }
                logger.error(err);
                resolve(false);
                return;
            }
            resolve(data);
        });                
    });
}

function publish_notification(topic, data) {
    return new Promise((resolve) => {
        const sns = new AWS.SNS();
        const topic_arn = 'arn:aws:sns:' + aws_region + ':' + aws_account + ':' + topic;
        const params = {
            Message: JSON.stringify(data),
            Subject: topic,
            TopicArn: topic_arn
        };
        sns.publish(params, function(err, data) {
            if (err) {
                logger.error(err);
                resolve(false);
                return;
            }
            resolve(data);
        });                
    });
}

function get_signed_url(bucket, key, expires = 900) {
    try {
        const s3 = new AWS.S3();
        const params = {Bucket: bucket, Key: key, Expires: expires};
        const url = s3.getSignedUrl('getObject', params);
        return url;
    } catch (err) {
        logger.error(err);
    }
    return null;
}

function file_exists(bucket, key) {
    return new Promise((resolve) => {
        try {
            const params = {Bucket: bucket, Key: key};
            const s3 = new AWS.S3();
            s3.headObject(params, function(err) {
                if (err) {
                    resolve(false);
                }
                return resolve(true);
            });
        } catch(err) {
            //logger.error(err);
            resolve(false);
        }
    });
}

function delete_file(bucket, key) {
    return new Promise((resolve) => {
        try {
            const params = {Bucket: bucket, key};
            const s3 = new AWS.S3();
            s3.deleteObjects(params, function(err, data) {
                if (err) {
                    logger.error(err);
                    resolve(false);
                }
                resolve(true);
            });
        } catch(err) {
            logger.error(err);
            resolve(false);
        }
    });
}

function delete_folder(bucket, prefix) {
    return new Promise((resolve) => {
        try {
            const params = {Bucket: bucket, Prefix: prefix};
            const s3 = new AWS.S3();
            s3.listObjects(params, function(err, data) {
                if (err) {
                    logger.error(err);
                    resolve(false);
                }
                if (data.Contents.length === 0) {
                    resolve(true);
                }
                const params = {Bucket: bucket, Delete: {Objects: []}};
                for (const content of data.Contents) {
                    params.Delete.Objects.push({Key: content.Key});
                }
                s3.deleteObjects(params, function(err, data) {
                    if (err) {
                        logger.error(err);
                        resolve(false);
                    }
                    resolve(true);
                });
            });
        } catch(err) {
            logger.error(err);
            resolve(false);
        }
    });
}

function upload_file(path, bucket, key) {
    return new Promise((resolve) => {
        try {
            const file = createReadStream(path);
            const pass = new stream.PassThrough();
            file.pipe(pass);
            const s3 = new AWS.S3();
            const params = {Bucket: bucket, Key: key, Body: pass};
            s3.upload(params, function(err) {
                if (err) {
                    logger.error(err);
                    resolve(false);
                }
                return resolve(true);
            });
        } catch(err) {
            logger.error(err);
            resolve(false);
        }
    });
}

async function upload_folder(path, bucket, prefix, concurrency = 8) {
    try {
        const list = listFolderRecursive(path);
        for (let i = 0; i < list.length; i += concurrency) {
            const promises = [];
            for (let j = 0; j < concurrency && i + j < list.length; j++) {
                const full_path = path + '/' + list[i+j];
                const key = prefix + '/' + list[i+j];
                promises.push(upload_file(full_path, bucket, key));
            }
            await Promise.all(promises);
        }
        return true;
    } catch(err) {
        logger.error(err);
        return false;
    }
}

function download_file(path, bucket, key) {
    return new Promise((resolve) => {
        const s3 = new AWS.S3();
        const params = {Bucket: bucket, Key: key};
        const s3_stream = s3.getObject(params).createReadStream();
        const file_stream = createWriteStream(path);
        s3_stream.on('error', function(err) {
            logger.error(err);
            resolve(false);
        });
        s3_stream.pipe(file_stream).on('error', function(err) {
            logger.error('File Stream:', err);
            resolve(false);
        }).on('close', function() {
            resolve(true);
        });
    });
}

async function register_task_definition(name, options) {
    return new Promise((resolve) => {
        const ecs = new AWS.ECS();
        let cpu = '256';
        let memory = '512';
        let family = name;
        let log_prefix = name;
        const environment = [];
        if (options) {
            for (const key in options) {
                if (key === 'cpu') {
                    cpu = String(options.cpu);
                    continue;
                }
                if (key === 'memory') {
                    memory = String(options.memory);
                    continue;
                }
                if (key === 'family') {
                    family = options.family;
                }
                if (key === 'log_prefix') {
                    log_prefix = options.log_prefix;
                }
                const value = String(options[key]);
                environment.push({name: key, value});
            }        
        }
        const image = aws_account + '.dkr.ecr.' + aws_region + '.amazonaws.com/' + name + ':latest';
        const essential = true;
        const executionRoleArn = 'arn:aws:iam::' + aws_account + ':role/ecs-task-role';
        const networkMode = 'awsvpc';
        const params = {
            family, cpu, memory, executionRoleArn, networkMode,
            requiresCompatibilities: [ 'FARGATE' ],
            containerDefinitions: [ {
                name, image, cpu, memory, environment, essential,
                healthCheck: {
                    "command": [ "CMD-SHELL", "echo hello" ],
                    "interval": 10,
                    "timeout": 3,
                    "retries": 3
                },
                logConfiguration: {
                    "logDriver": "awslogs",
                    "options": {
                        "awslogs-group": name,
                        "awslogs-region": aws_region,
                        "awslogs-stream-prefix": log_prefix
                    }
                }
            }]
        };
        ecs.registerTaskDefinition(params, function(err, data) {
            if (err) {
                logger.error(err);
                resolve(false);
            } else {
                if (data && data.taskDefinition && data.taskDefinition.taskDefinitionArn) {
                    const parts = data.taskDefinition.taskDefinitionArn.split('/');
                    const task_definition = parts[parts.length - 1];
                    resolve(task_definition);
                } else {
                    logger.error('register_task_definition', 'unexpected data', data);
                    resolve(false);
                }
            }
        });
    });
}

async function deregister_task_definition(task_definition) {
    return new Promise((resolve) => {
        const ecs = new AWS.ECS();
        const params = { taskDefinition: task_definition };
        ecs.deregisterTaskDefinition(params, function(err, data) {
            if (err) {
                logger.error(err);
                resolve(false);
            } else {
                //logger.trace(JSON.stringify(data, null, 2));
                resolve(true);
            }
        });
    });
}

async function create_ecs_service(name, task_name, task_definition) {
    return new Promise((resolve) => {
        const ecs = new AWS.ECS();
        const params = {
            cluster: name,
            serviceName: task_name, 
            taskDefinition: task_definition,
            launchType: 'FARGATE',
            desiredCount: 1,
            networkConfiguration: {
                awsvpcConfiguration: {
                  subnets: aws_private_subnets,
                  //assignPublicIp: 'ENABLED',
                  securityGroups: aws_security_groups
                }
            },
        };
        ecs.createService(params, function(err, data) {
            if (err) {
                logger.error(err);
                resolve(false);
            } else {
                //logger.trace(JSON.stringify(data, null, 2));
                resolve(true);
            }
        });
    });
}

async function stop_ecs_service(name, store_name, task_definition) {
    return new Promise((resolve) => {
        const ecs = new AWS.ECS();
        const params = {
            cluster: name,
            service: store_name, 
            taskDefinition: task_definition,
            desiredCount: 0
        };
        ecs.updateService(params, function(err, data) {
            if (err) {
                logger.error(err);
                resolve(false);
            } else {
                //logger.trace(JSON.stringify(data, null, 2));
                resolve(true);
            }
        });
    });
}

async function delete_ecs_service(name, store_name) {
    return new Promise((resolve) => {
        const ecs = new AWS.ECS();
        const params = {
            cluster: name,
            service: store_name, 
        };
        ecs.deleteService(params, function(err, data) {
            if (err) {
                logger.error(err);
                resolve(false);
            } else {
                //logger.trace(JSON.stringify(data, null, 2));
                resolve(true);
            }
        });
    });
}

function get_queue_size_approximation(queue) {
    const queue_url = queue.startsWith('https://') ? queue : aws_queue_urls[queue];
    if (!queue_url) {
        throw Error('queue_name not found: ' + queue);
    }
    return new Promise((resolve) => {
        const sqs = new AWS.SQS();
        const params = {
            QueueUrl: queue_url,
            AttributeNames : [ 'ApproximateNumberOfMessages' ],
        };
        sqs.getQueueAttributes(params, function(err, data) {
            if (err) {
                logger.error(err);
                resolve(null);
            } else {
                if (data && data.Attributes && data.Attributes.ApproximateNumberOfMessages) {
                    const size = Number(data.Attributes.ApproximateNumberOfMessages);
                    resolve(size);
                } else {
                    logger.error('unexpected case: ', data);
                    resolve(0);
                }
            }
        });
    });

}

function send_sqs_message(queue, message) {
    const queue_url = queue.startsWith('https://') ? queue : aws_queue_urls[queue];
    if (!queue_url) {
        throw Error('queue_name not found: ' + queue);
    }
    return new Promise((resolve) => {
        const sqs = new AWS.SQS();
        const params = {
            MessageBody: JSON.stringify(message),
            QueueUrl: queue_url
        };
        sqs.sendMessage(params, function(err, data) {
            if (err) {
                logger.error(err);
                resolve(false);
            } else {
                resolve(true);
            }
        });
    });
}

function get_sqs_messages(queue, max_messages = 10, visibility_timeout = 3, delete_message = true, wait = 0) {
    const queue_url = queue.startsWith('https://') ? queue : aws_queue_urls[queue];
    if (!queue_url) {
        throw Error('queue_name not found: ' + queue);
    }
    return new Promise((resolve) => {
        const sqs = new AWS.SQS();
        const params = {
            AttributeNames: [ "SentTimestamp" ],
            MaxNumberOfMessages: max_messages,
            QueueUrl: queue_url,
            VisibilityTimeout: visibility_timeout,
            WaitTimeSeconds: wait
        };
        sqs.receiveMessage(params, function(err, data) {
            if (err) {
                logger.error(err);
                resolve(false);
            } else {
                if (data.Messages && data.Messages.length > 0) {
                    if (delete_message) {
                        delete_sqs_messages(queue, data.Messages).then(() => {
                            resolve(data.Messages);
                        }).catch((err) => {
                            logger.error(err);
                            resolve(false);
                        });
                    } else {
                        resolve(data.Messages);
                    }
                } else {
                    resolve([]);
                }
            }
        });
    });
}

function delete_sqs_messages(queue, messages) {
    const promises = [];
    for (const message of messages) {
        promises.push(delete_sqs_message(queue, message));
    }
    return Promise.all(promises);
}

function delete_sqs_message(queue, message) {
    const queue_url = queue.startsWith('https://') ? queue : aws_queue_urls[queue];
    if (!queue_url) {
        throw Error('queue_name not found: ' + queue);
    }
    return new Promise((resolve) => {
        const sqs = new AWS.SQS();
        const params = {
            QueueUrl: queue_url,
            ReceiptHandle: message.ReceiptHandle
        };
        sqs.deleteMessage(params, function(err, data) {
            if (err) {
                logger.error(err);
                resolve(false);
            } else {
                resolve(true);
            }
        });
    });
}
