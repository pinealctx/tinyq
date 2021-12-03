SET NAMES 'utf8mb4';

CREATE TABLE `topic_example`
(
    `id`         bigint      NOT NULL COMMENT 'id',
    `p`          int         NOT NULL COMMENT '单号，幂等',
    `data`       mediumblob  NOT NULL COMMENT '日志',
    `created_at` datetime    NOT NULL COMMENT '创建时间',
    PRIMARY KEY (`id`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_bin;

CREATE TABLE `topic_cursor_example`
(
    `cid`        varchar(256)  NOT NULL COMMENT '客户端ID',
    `sid`        bigint        NOT NULL COMMENT '消费到的ID',
    `updated_at` datetime      NOT NULL COMMENT '更新时间',
    PRIMARY KEY (`cid`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_bin;
