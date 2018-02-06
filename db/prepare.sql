/*
Connection表
ClientID（string），nodeId，time
ClientID是主键

subscribe表
topic，ClientID，qos
topic，ClientID是主键；索引：1，topic，2，ClientID

message表
Id，message，time

message-list表
ClientID，message-id，time
ClientID，message-id是主键，索引：ClientID


retain表
topic，message
*/

create database mqtt;
use mqtt;


CREATE TABLE `connection` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `client_id` varchar(32) NOT NULL,
  `node_id` varchar(32) NOT NULL,
  `time` datetime(6) NOT NULL,
  PRIMARY KEY (`id`),
  KEY `client_id` (`client_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


CREATE TABLE `subscribe` (
  `topic` varchar(128) NOT NULL,
  `client_id` varchar(32) NOT NULL,
  `qos` tinyint(4) NOT NULL DEFAULT '0',
  `time` datetime(6) NOT NULL,
  PRIMARY KEY `client_id` (`client_id`, `topic`),
  KEY `topic` (`topic`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


CREATE TABLE `message` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `topic` varchar(128) NOT NULL,
  `message` text NOT NULL,
  `packet_id` int(11) NOT NULL DEFAULT 0,
  `client_id` varchar(32) NOT NULL,
  `time` datetime(6) NOT NULL,
  PRIMARY KEY (`id`),
  KEY `packet_id` (`packet_id`, `client_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


CREATE TABLE `msg_list` (
  `client_id` varchar(32) NOT NULL,
  `message_id` bigint NOT NULL,
  `qos` tinyint(4) NOT NULL DEFAULT '0',
  `status` tinyint(4) NOT NULL DEFAULT '0',
  `time` datetime(6) NOT NULL,
  PRIMARY KEY (`client_id`, `message_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;





