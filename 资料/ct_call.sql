/*
 Navicat Premium Data Transfer

 Source Server         : jungle-mysql
 Source Server Type    : MySQL
 Source Server Version : 50722
 Source Host           : 192.168.1.18:8806
 Source Schema         : ct

 Target Server Type    : MySQL
 Target Server Version : 50722
 File Encoding         : 65001

 Date: 20/11/2019 17:07:05
*/

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
-- Table structure for ct_call
-- ----------------------------
DROP TABLE IF EXISTS `ct_call`;
CREATE TABLE `ct_call`  (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `telid` int(11) NULL DEFAULT NULL,
  `dateid` int(11) NULL DEFAULT NULL,
  `sumcall` int(11) NULL DEFAULT 0,
  `sumduration` int(11) NULL DEFAULT 0,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci ROW_FORMAT = Dynamic;

