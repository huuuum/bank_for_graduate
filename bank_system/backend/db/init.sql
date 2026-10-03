
-- 1. 客户总体统计表（离线 Spark 统计写库）
CREATE TABLE IF NOT EXISTS overall_stats (
    id                   INT NOT NULL AUTO_INCREMENT COMMENT '自增主键',
    total_customers      INT NOT NULL COMMENT '总客户数',
    subscribed_count     INT NOT NULL COMMENT '认购客户数',
    not_subscribed_count INT NOT NULL COMMENT '未认购客户数',
    subscribe_rate       DECIMAL(5,4) NOT NULL COMMENT '认购率',
    avg_age              DECIMAL(5,2) DEFAULT NULL COMMENT '平均年龄',
    avg_duration         DECIMAL(8,2) DEFAULT NULL COMMENT '平均通话时长(秒)',
    stat_time            DATETIME NOT NULL COMMENT '统计时间',
    PRIMARY KEY (id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='客户总体统计表';

-- 2. 年龄分布统计表
CREATE TABLE IF NOT EXISTS age_stats (
    id              INT NOT NULL AUTO_INCREMENT COMMENT '自增主键',
    age_group       VARCHAR(20) NOT NULL COMMENT '年龄段(如 20-29)',
    customer_count  INT NOT NULL COMMENT '该年龄段客户数',
    subscribe_count INT NOT NULL COMMENT '该年龄段认购数',
    PRIMARY KEY (id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='年龄分布统计表';

-- 3. 职业分布统计表
CREATE TABLE IF NOT EXISTS job_stats (
    id              INT NOT NULL AUTO_INCREMENT COMMENT '自增主键',
    job             VARCHAR(50) NOT NULL COMMENT '职业',
    customer_count  INT NOT NULL COMMENT '该职业客户数',
    subscribe_count INT NOT NULL COMMENT '该职业认购数',
    PRIMARY KEY (id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='职业分布统计表';

-- 4. 婚姻状况分布统计表
CREATE TABLE IF NOT EXISTS marital_stats (
    id              INT NOT NULL AUTO_INCREMENT COMMENT '自增主键',
    marital         VARCHAR(20) NOT NULL COMMENT '婚姻状况',
    customer_count  INT NOT NULL COMMENT '客户数',
    subscribe_count INT NOT NULL COMMENT '认购数',
    PRIMARY KEY (id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='婚姻状况分布统计表';

-- 5. 教育程度分布统计表
CREATE TABLE IF NOT EXISTS education_stats (
    id              INT NOT NULL AUTO_INCREMENT COMMENT '自增主键',
    education       VARCHAR(50) NOT NULL COMMENT '教育程度',
    customer_count  INT NOT NULL COMMENT '客户数',
    subscribe_count INT NOT NULL COMMENT '认购数',
    PRIMARY KEY (id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='教育程度分布统计表';

-- 6. 联系月份分布统计表
CREATE TABLE IF NOT EXISTS month_stats (
    id              INT NOT NULL AUTO_INCREMENT COMMENT '自增主键',
    month           VARCHAR(10) NOT NULL COMMENT '最后联系月份(如 aug)',
    customer_count  INT NOT NULL COMMENT '该月份联系客户数',
    subscribe_count INT NOT NULL COMMENT '该月份认购数',
    PRIMARY KEY (id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='联系月份分布统计表';

-- 7. 实时预测结果表（PyFlink 实时推理写库）
CREATE TABLE IF NOT EXISTS realtime_prediction (
    id                  BIGINT NOT NULL AUTO_INCREMENT COMMENT '自增主键',
    customer_id         INT NOT NULL COMMENT '客户ID',
    age                 INT DEFAULT NULL COMMENT '年龄',
    job                 VARCHAR(50) DEFAULT NULL COMMENT '职业',
    marital             VARCHAR(20) DEFAULT NULL COMMENT '婚姻状况',
    education           VARCHAR(50) DEFAULT NULL COMMENT '教育程度',
    housing             VARCHAR(10) DEFAULT NULL COMMENT '住房贷款',
    loan                VARCHAR(10) DEFAULT NULL COMMENT '个人贷款',
    duration            INT DEFAULT NULL COMMENT '通话时长(秒)',
    campaign            INT DEFAULT NULL COMMENT '本次营销联系次数',
    predict_result      VARCHAR(10) NOT NULL COMMENT '预测结果(yes/no)',
    predict_probability DECIMAL(5,4) DEFAULT NULL COMMENT '认购概率',
    predict_time        DATETIME NOT NULL COMMENT '预测时间',
    PRIMARY KEY (id),
    UNIQUE KEY uk_customer (customer_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='实时预测结果表';

-- 8. 客户基础信息表（方案B新增，存 stream_10.csv 全量 22 列）
CREATE TABLE IF NOT EXISTS customer (
    id               INT PRIMARY KEY COMMENT '客户ID(来自csv，业务主键)',
    age              INT COMMENT '年龄',
    job              VARCHAR(50) COMMENT '职业',
    marital          VARCHAR(20) COMMENT '婚姻状况',
    education        VARCHAR(50) COMMENT '教育程度',
    `default`        VARCHAR(10) COMMENT '是否有违约记录(no/yes/unknown)',
    housing          VARCHAR(10) COMMENT '是否有住房贷款',
    loan             VARCHAR(10) COMMENT '是否有个人贷款',
    contact          VARCHAR(20) COMMENT '联系方式(cellular/telephone)',
    month            VARCHAR(10) COMMENT '最后联系月份',
    day_of_week      VARCHAR(10) COMMENT '最后联系星期几',
    duration         INT COMMENT '通话时长(秒)',
    campaign         INT COMMENT '本次营销联系次数',
    pdays            INT COMMENT '距上次联系间隔天数(999=从未)',
    previous         INT COMMENT '过去营销联系次数',
    poutcome         VARCHAR(20) COMMENT '过去营销结果',
    emp_var_rate     DOUBLE COMMENT '就业变化率',
    cons_price_index DOUBLE COMMENT '消费者价格指数',
    cons_conf_index  DOUBLE COMMENT '消费者信心指数',
    lending_rate3m   DOUBLE COMMENT '3个月同业拆借利率',
    nr_employed      DOUBLE COMMENT '员工数量',
    subscribe        VARCHAR(10) COMMENT '是否认购定期存款(真实标签 yes/no)'
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='客户基础信息表';