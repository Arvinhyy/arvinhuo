-- INVALIDATE METADATA;

DROP TABLE IF EXISTS wp_man.loan_application_approval_type;
CREATE TABLE wp_man.loan_application_approval_type (
  id int,
  code string,
  description string,
  type string
) STORED AS PARQUET;

INSERT INTO wp_man.loan_application_approval_type VALUES
(1, "normal", "正常", "正常"),
(2, "semi-auto", "半自动", "半自动"),
(3, "semi-auto-final", "半自动审批完成", "半自动"),
(4, "experiment_gjj", "公积金测试一期", "实验件"),
(5, "experiment_gjjupgrade", "公积金用户升级", "实验件"),
(6, "experiment_kaniu", "卡牛测试一期", "实验件"),
(7, "experiment_pbcredit", "人行征信测试一期", "实验件"),
(8, "experiment_ylzh", "银联智慧一期", "实验件"),
(9, "experiment_shd", "商户贷类型", "实验件"),
(10, "manual", "人工处理", "人工"),
(11, "wedefend", "全自动", "全自动");

COMPUTE STATS wp_man.loan_application_approval_type;

DROP VIEW IF EXISTS wp_calc.loan_application_approval_type;
CREATE VIEW wp_calc.loan_application_approval_type AS SELECT * FROM wp_man.loan_application_approval_type;

DROP VIEW IF EXISTS wp_biz.loan_application_approval_type;
CREATE VIEW wp_biz.loan_application_approval_type AS SELECT * FROM wp_man.loan_application_approval_type;

DROP VIEW IF EXISTS weplay.loan_application_approval_type;
CREATE VIEW weplay.loan_application_approval_type AS SELECT * FROM wp_man.loan_application_approval_type;

DROP TABLE IF EXISTS wp_man.loan_application_state;
CREATE TABLE wp_man.loan_application_state (
  id int,
  code string,
  description string,
  is_backlog int,
  is_rjct int,
  is_aprv int,
  is_deal int,
  is_disb int,
  is_close int,
  is_early int
) STORED AS PARQUET;

INSERT INTO wp_man.loan_application_state VALUES
(1, "pretrial_applied", "已预审", 1, 0, 0, 0, 0, 0, 0),
(2, "applied", "已申请", 1, 0, 0, 0, 0, 0, 0),
(3, "init_aip", "已初审", 1, 0, 0, 0, 0, 0, 0),
(4, "push_backed", "已退回", 0, 0, 0, 0, 0, 0, 0),
(5, "aip", "已终审", 0, 0, 1, 1, 0, 0, 0),
(6, "pending_aip", "待第三方审核", 0, 0, 1, 1, 0, 0, 0),
(7, "confirmed", "已确认", 0, 0, 1, 1, 0, 0, 0),
(8, "funded", "已配标", 0, 0, 1, 1, 0, 0, 0),
(9, "disburse_failed", "放款失败", 0, 0, 1, 1, 0, 0, 0),
(10, "withdraw_applied", "提现申请", 0, 0, 1, 1, 0, 0, 0),
(11, "disbursed", "已放款", 0, 0, 1, 1, 1, 0, 0),
(12, "closed", "已结清", 0, 0, 1, 1, 1, 1, 0),
(13, "early_settled", "提前结清", 0, 0, 1, 1, 1, 1, 1),
(14, "rejected", "已拒绝", 0, 1, 0, 1, 0, 0, 0),
(15, "cancelled", "已取消", 0, 0, 0, 0, 0, 0, 0);

COMPUTE STATS wp_man.loan_application_state;

DROP VIEW IF EXISTS wp_calc.loan_application_state;
CREATE VIEW wp_calc.loan_application_state AS SELECT * FROM wp_man.loan_application_state;

DROP VIEW IF EXISTS wp_biz.loan_application_state;
CREATE VIEW wp_biz.loan_application_state AS SELECT * FROM wp_man.loan_application_state;

DROP VIEW IF EXISTS weplay.loan_application_state;
CREATE VIEW weplay.loan_application_state AS SELECT * FROM wp_man.loan_application_state;

DROP TABLE IF EXISTS wp_man.product_info;
CREATE TABLE wp_man.product_info
(
    id int,
    name string,
    code string,
    prod_code string,
    prod_name string,
    level string,
    platform string,
    category string,
    dept string,
    credit_type string,
    enable int
);

INSERT INTO wp_man.product_info VALUES
(1, "学生族", "WLD", "WLD", "我来贷APP", "其他", "APP", "TC标准", "个金", "normal", 1),
(2, "酷派", "CoolPad", "coolpad", "酷派", "其他", "其他", "TC标准", "个金", "normal", 1),
(3, "7天免息", "WLD-Fee-Free", "WLD", "我来贷APP", "其他", "APP", "TC标准", "个金", "normal", 1),
(4, "极速放款", "WLD-Speed-Up", "WLD", "我来贷APP", "其他", "APP", "TC标准", "个金", "normal", 1),
(5, "工薪族A级", "WLD-Staff-A", "WLD", "我来贷APP", "A", "APP", "TC标准", "个金", "normal", 1),
(6, "邮乐贷-学生族", "ULE-Student", "ULE", "邮乐", "其他", "APP", "TC标准", "个金", "normal", 1),
(7, "邮乐贷-工薪族", "ULE-Staff", "ULE", "邮乐", "其他", "APP", "TC标准", "个金", "normal", 1),
(8, "邮乐贷-极速放款", "ULE-Speed-Up", "ULE", "邮乐", "其他", "APP", "TC标准", "个金", "normal", 1),
(9, "人人分期", "RRFQ", "RRFQ", "人人", "其他", "其他", "TC标准", "个金", "normal", 1),
(10, "工薪族B级", "WLD-Staff-B", "WLD", "我来贷APP", "B", "APP", "TC标准", "个金", "normal", 1),
(11, "工薪族C级", "WLD-Staff-C", "WLD", "我来贷APP", "C", "APP", "TC标准", "个金", "normal", 1),
(12, "工薪族A+级", "WLD-Staff-A+", "WLD", "我来贷APP", "A", "APP", "TC标准", "个金", "normal", 1),
(13, "工薪族B+级", "WLD-Staff-B+", "WLD", "我来贷APP", "B", "APP", "TC标准", "个金", "normal", 1),
(14, "闪电贷", "WLD-Student-Lightning", "WLD", "我来贷APP", "其他", "H5", "TC标准", "个金", "normal", 1),
(15, "闪电贷-工薪族A级", "WLD-Staff-A-Lightning", "WLD", "我来贷APP", "A", "H5", "TC标准", "个金", "normal", 1),
(16, "简易贷-学生族", "WLD-Student-Easy", "WLD-Student-Easy", "简易贷", "其他", "APP", "TC标准", "个金", "normal", 1),
(17, "简易贷-工薪族", "WLD-Staff-Easy", "WLD-Staff-Easy", "简易贷", "其他", "APP", "TC标准", "个金", "normal", 1),
(19, "融360-学生族", "WLD-Student-Rong360", "WLD-Student-Rong361", "融360", "其他", "API", "TC标准", "个金", "normal", 1),
(20, "融360-工薪族-C", "WLD-Staff-Rong360-C", "WLD-Staff-Rong360", "融360", "C", "API", "TC标准", "个金", "normal", 1),
(21, "融360-工薪族-B", "WLD-Staff-Rong360-B", "WLD-Staff-Rong360", "融360", "B", "API", "TC标准", "个金", "normal", 1),
(22, "融360-工薪族-A", "WLD-Staff-Rong360-A", "WLD-Staff-Rong360", "融360", "A", "API", "TC标准", "个金", "normal", 1),
(23, "卡牛-工薪族-A", "Kaniu-Staff-A", "Kaniu-Staff", "卡牛", "A", "API", "TC标准", "个金", "normal", 1),
(24, "卡牛-工薪族-B", "Kaniu-Staff-B", "Kaniu-Staff", "卡牛", "B", "API", "TC标准", "个金", "normal", 1),
(25, "卡牛-工薪族-C", "Kaniu-Staff-C", "Kaniu-Staff", "卡牛", "C", "API", "TC标准", "个金", "normal", 1),
(26, "乐富-学生族", "WLD-Student-Lefu", "LEFU", "乐富", "其他", "APP", "TC标准", "个金", "normal", 1),
(27, "乐富-工薪族-C", "WLD-Staff-Lefu-C", "LEFU", "乐富", "C", "APP", "TC标准", "个金", "normal", 1),
(28, "乐富-工薪族-B", "WLD-Staff-Lefu-B", "LEFU", "乐富", "B", "APP", "TC标准", "个金", "normal", 1),
(29, "乐富-工薪族-A", "WLD-Staff-Lefu-A", "LEFU", "乐富", "A", "APP", "TC标准", "个金", "normal", 1),
(30, "随手记-工薪族-A", "Feidee-Staff-A", "Feidee-Staff", "随手记", "A", "API", "TC标准", "个金", "normal", 1),
(31, "随手记-工薪族-B", "Feidee-Staff-B", "Feidee-Staff", "随手记", "B", "API", "TC标准", "个金", "normal", 1),
(32, "随手记-工薪族-C", "Feidee-Staff-C", "Feidee-Staff", "随手记", "C", "API", "TC标准", "个金", "normal", 1),
(33, "安家周转金", "ANJIA", "Feidee-Staff", "安家金", "其他", "其他", "TC标准", "个金", "normal", 1),
(34, "我来贷H5-工薪族-A", "H5-Staff-A", "H5-Staff", "我来贷H5", "A", "H5", "TC标准", "个金", "normal", 1),
(35, "我来贷H5-工薪族-B", "H5-Staff-B", "H5-Staff", "我来贷H5", "B", "H5", "TC标准", "个金", "normal", 1),
(36, "我来贷H5-工薪族-C", "H5-Staff-C", "H5-Staff", "我来贷H5", "C", "H5", "TC标准", "个金", "normal", 1),
(37, "我来贷短期（1M）-学生", "WLD-Shortterm-Student", "WLD-Shortterm-Student", "千元免息", "其他", "APP", "TC标准", "个金", "normal", 1),
(38, "我来贷短期（1M）-工薪A", "WLD-Shortterm-Staff-A", "WLD-Shortterm-Staff", "千元免息", "A", "APP", "TC标准", "个金", "normal", 1),
(39, "我来贷短期（1M）-工薪B", "WLD-Shortterm-Staff-B", "WLD-Shortterm-Staff", "千元免息", "B", "APP", "TC标准", "个金", "normal", 1),
(40, "我来贷短期（1M）-工薪C", "WLD-Shortterm-Staff-C", "WLD-Shortterm-Staff", "千元免息", "C", "APP", "TC标准", "个金", "normal", 1),
(41, "卡牛简易版A-工薪族A", "Kaniu-EasyA-Staff-A", "Kaniu", "卡牛", "A", "API", "TC标准", "个金", "normal", 1),
(42, "卡牛简易版A-工薪族B", "Kaniu-EasyA-Staff-B", "Kaniu", "卡牛", "B", "API", "TC标准", "个金", "normal", 1),
(43, "卡牛简易版A-工薪族C", "Kaniu-EasyA-Staff-C", "Kaniu", "卡牛", "C", "API", "TC标准", "个金", "normal", 1),
(44, "卡牛简易版B-工薪族A", "Kaniu-EasyB-Staff-A", "Kaniu", "卡牛", "A", "API", "TC标准", "个金", "normal", 1),
(45, "卡牛简易版B-工薪族B", "Kaniu-EasyB-Staff-B", "Kaniu", "卡牛", "B", "API", "TC标准", "个金", "normal", 1),
(46, "卡牛简易版B-工薪族C", "Kaniu-EasyB-Staff-C", "Kaniu", "卡牛", "C", "API", "TC标准", "个金", "normal", 1),
(47, "薪粤贷", "XYD-LargeAmount", "XYD-LargeAmount", "薪粤贷", "其他", "其他", "TC标准", "个金", "normal", 1),
(48, "安信贷", "AX-LargeAmount", "AX-LargeAmount", "安信贷", "其他", "其他", "TC标准", "个金", "normal", 1),
(49, "我来贷SDK-A", "WLD-SDK-A", "WLD-SDK", "我来贷SDK", "A", "SDK", "TC标准", "个金", "normal", 1),
(50, "打工贷", "PDL", "PDL", "打工贷", "其他", "其他", "TC标准", "个金", "normal", 1),
(51, "我来贷SDK-B", "WLD-SDK-B", "WLD-SDK", "我来贷SDK", "B", "SDK", "TC标准", "个金", "normal", 1),
(52, "我来贷SDK-C", "WLD-SDK-C", "WLD-SDK", "我来贷SDK", "C", "SDK", "TC标准", "个金", "normal", 1),
(53, "工薪族D级", "WLD-Staff-D", "WLD", "我来贷APP", "D", "APP", "TC标准", "个金", "normal", 1),
(54, "融360-工薪族-D", "WLD-Staff-Rong360-D", "WLD-Staff-Rong360", "融360", "D", "API", "TC标准", "个金", "normal", 1),
(55, "卡牛-工薪族-D", "Kaniu-Staff-D", "Kaniu-Staff", "卡牛", "D", "API", "TC标准", "个金", "normal", 1),
(56, "乐富-工薪族-D", "WLD-Staff-Lefu-D", "LEFU", "乐富", "D", "APP", "TC标准", "个金", "normal", 1),
(57, "随手记-工薪族-D", "Feidee-Staff-D", "Feidee-Staff", "随手记", "D", "API", "TC标准", "个金", "normal", 1),
(58, "我来贷H5-工薪族-D", "H5-Staff-D", "H5-Staff", "我来贷H5", "D", "H5", "TC标准", "个金", "normal", 1),
(59, "我来贷短期（1M）-工薪D", "WLD-Shortterm-Staff-D", "WLD-Shortterm-Staff", "千元免息", "D", "APP", "TC标准", "个金", "normal", 1),
(60, "卡牛简易版A-工薪族D", "Kaniu-EasyA-Staff-D", "Kaniu-Staff", "卡牛", "D", "API", "TC标准", "个金", "normal", 1),
(61, "卡牛简易版B-工薪族D", "Kaniu-EasyB-Staff-D", "Kaniu-Staff", "卡牛", "D", "API", "TC标准", "个金", "normal", 1),
(62, "我来贷SDK-D", "WLD-SDK-D", "WLD-SDK", "我来贷SDK", "D", "SDK", "TC标准", "个金", "normal", 1),
(63, "我来贷闪电贷-A", "SDD-WLD-Staff-A", "WLD-SDD", "闪电贷", "A", "APP", "TC标准", "个金", "credit", 1),
(64, "我来贷闪电贷-B", "SDD-WLD-Staff-B", "WLD-SDD", "闪电贷", "B", "APP", "TC标准", "个金", "credit", 1),
(65, "我来贷闪电贷-C", "SDD-WLD-Staff-C", "WLD-SDD", "闪电贷", "C", "APP", "TC标准", "个金", "credit", 1),
(66, "我来贷闪电贷-D", "SDD-WLD-Staff-D", "WLD-SDD", "闪电贷", "D", "APP", "TC标准", "个金", "credit", 1),
(67, "H5工薪简单贷A", "H5-Staff-JDD-A", "H5-Staff-JDD", "简单贷V1", "A", "H5", "TC标准", "个金", "normal", 1),
(68, "H5工薪简单贷B", "H5-Staff-JDD-B", "H5-Staff-JDD", "简单贷V1", "B", "H5", "TC标准", "个金", "normal", 1),
(69, "H5工薪简单贷C", "H5-Staff-JDD-C", "H5-Staff-JDD", "简单贷V1", "C", "H5", "TC标准", "个金", "normal", 1),
(70, "H5工薪简单贷D", "H5-Staff-JDD-D", "H5-Staff-JDD", "简单贷V1", "D", "H5", "TC标准", "个金", "normal", 1),
(71, "富士康", "FOXCONN", "FOXCONN", "富士康", "其他", "其他", "TC标准", "个金", "normal", 1),
(72, "商户贷A级", "SHD-A", "SHD-A", "商户贷", "A", "H5", "TC非标", "个金", "credit", 1),
(73, "商户贷B级", "SHD-B", "SHD-B", "商户贷", "B", "H5", "TC非标", "个金", "credit", 1),
(74, "商户贷C级", "SHD-C", "SHD-C", "商户贷", "C", "H5", "TC非标", "个金", "credit", 1),
(75, "商户贷D级", "SHD-D", "SHD-D", "商户贷", "D", "H5", "TC非标", "个金", "credit", 1),
(76, "H5闪电贷-A", "H5-SDD-A", "H5-SDD", "闪电贷", "A", "H5", "TC标准", "个金", "credit", 1),
(77, "H5闪电贷-B", "H5-SDD-B", "H5-SDD", "闪电贷", "B", "H5", "TC标准", "个金", "credit", 1),
(78, "H5闪电贷-C", "H5-SDD-C", "H5-SDD", "闪电贷", "C", "H5", "TC标准", "个金", "credit", 1),
(79, "H5闪电贷-D", "H5-SDD-D", "H5-SDD", "闪电贷", "D", "H5", "TC标准", "个金", "credit", 1),
(80, "H5公积金专案A", "GJJ-H5-A", "GJJ-H5", "公积金专案", "A", "H5", "TC标准", "个金", "normal", 1),
(81, "H5公积金专案B", "GJJ-H5-B", "GJJ-H5", "公积金专案", "B", "H5", "TC标准", "个金", "normal", 1),
(82, "H5公积金专案C", "GJJ-H5-C", "GJJ-H5", "公积金专案", "C", "H5", "TC标准", "个金", "normal", 1),
(83, "H5公积金专案D", "GJJ-H5-D", "GJJ-H5", "公积金专案", "D", "H5", "TC标准", "个金", "normal", 1),
(84, "APP公积金专案A", "GJJ-APP-A", "GJJ-APP", "公积金专案", "A", "APP", "TC标准", "个金", "normal", 1),
(85, "APP公积金专案B", "GJJ-APP-B", "GJJ-APP", "公积金专案", "B", "APP", "TC标准", "个金", "normal", 1),
(86, "APP公积金专案C", "GJJ-APP-C", "GJJ-APP", "公积金专案", "C", "APP", "TC标准", "个金", "normal", 1),
(87, "APP公积金专案D", "GJJ-APP-D", "GJJ-APP", "公积金专案", "D", "APP", "TC标准", "个金", "normal", 1),
(88, "银行版简单贷A", "YHD-GJJ-A", "YHD-GJJ", "银行大额", "A", "APP", "TC标准", "个金", "normal", 1),
(89, "银行版简单贷B", "YHD-GJJ-B", "YHD-GJJ", "银行大额", "B", "APP", "TC标准", "个金", "normal", 1),
(90, "银行版简单贷C", "YHD-GJJ-C", "YHD-GJJ", "银行大额", "C", "APP", "TC标准", "个金", "normal", 1),
(91, "银行版简单贷D", "YHD-GJJ-D", "YHD-GJJ", "银行大额", "D", "APP", "TC标准", "个金", "normal", 1),
(92, "H5简单贷V2-A", "H5-JDDV2-A", "H5-JDDV2", "简单贷V2", "A", "H5", "TC标准", "个金", "normal", 1),
(93, "H5简单贷V2-B", "H5-JDDV2-B", "H5-JDDV2", "简单贷V2", "B", "H5", "TC标准", "个金", "normal", 1),
(94, "H5简单贷V2-C", "H5-JDDV2-C", "H5-JDDV2", "简单贷V2", "C", "H5", "TC标准", "个金", "normal", 1),
(95, "H5简单贷V2-D", "H5-JDDV2-D", "H5-JDDV2", "简单贷V2", "D", "H5", "TC标准", "个金", "normal", 1),
(125, "H5简单贷V3-A", "H5-JDDV3-A", "H5-JDDV3", "简单贷V3", "A", "H5", "TC标准", "个金", "normal", 1),
(126, "H5简单贷V3-B", "H5-JDDV3-B", "H5-JDDV3", "简单贷V3", "B", "H5", "TC标准", "个金", "normal", 1),
(127, "H5简单贷V3-C", "H5-JDDV3-C", "H5-JDDV3", "简单贷V3", "C", "H5", "TC标准", "个金", "normal", 1),
(128, "H5简单贷V3-D", "H5-JDDV3-D", "H5-JDDV3", "简单贷V3", "D", "H5", "TC标准", "个金", "normal", 1),
(158, "APP简单贷-A", "APP-JDD-A", "APP-JDD", "APP简单贷", "A", "APP", "TC标准", "个金", "normal", 1),
(159, "APP简单贷-B", "APP-JDD-B", "APP-JDD", "APP简单贷", "B", "APP", "TC标准", "个金", "normal", 1),
(160, "APP简单贷-C", "APP-JDD-C", "APP-JDD", "APP简单贷", "C", "APP", "TC标准", "个金", "normal", 1),
(161, "APP简单贷-D", "APP-JDD-D", "APP-JDD", "APP简单贷", "D", "APP", "TC标准", "个金", "normal", 1),
(162, "APP信用卡贷-A", "APP-XYKD-A", "APP-XYKD", "APP信用卡贷", "A", "APP", "TC标准", "个金", "normal", 1),
(163, "APP信用卡贷-B", "APP-XYKD-B", "APP-XYKD", "APP信用卡贷", "B", "APP", "TC标准", "个金", "normal", 1),
(164, "APP信用卡贷-C", "APP-XYKD-C", "APP-XYKD", "APP信用卡贷", "C", "APP", "TC标准", "个金", "normal", 1),
(165, "APP信用卡贷-D", "APP-XYKD-D", "APP-XYKD", "APP信用卡贷", "D", "APP", "TC标准", "个金", "normal", 1),
(166, "APP公积金贷-A", "APP-GJJD-A", "APP-GJJD", "APP公积金贷", "A", "APP", "TC标准", "个金", "normal", 1),
(167, "APP公积金贷-B", "APP-GJJD-B", "APP-GJJD", "APP公积金贷", "B", "APP", "TC标准", "个金", "normal", 1),
(168, "APP公积金贷-C", "APP-GJJD-C", "APP-GJJD", "APP公积金贷", "C", "APP", "TC标准", "个金", "normal", 1),
(169, "APP公积金贷-D", "APP-GJJD-D", "APP-GJJD", "APP公积金贷", "D", "APP", "TC标准", "个金", "normal", 1),
(170, "H5-电商贷-A", "H5-DSD-A", "H5-DSD", "H5-电商贷", "A", "H5", "智金大额", "智金", "normal", 1),
(171, "H5-电商贷-B", "H5-DSD-B", "H5-DSD", "H5-电商贷", "B", "H5", "智金大额", "智金", "normal", 1),
(172, "H5-电商贷-C", "H5-DSD-C", "H5-DSD", "H5-电商贷", "C", "H5", "智金大额", "智金", "normal", 1),
(173, "H5-电商贷-D", "H5-DSD-D", "H5-DSD", "H5-电商贷", "D", "H5", "智金大额", "智金", "normal", 1),
(174, "贝贝-A", "CREDIT-STAFF-BEIBEI-A", "CREDIT-STAFF-BEIBEI", "贝贝", "A", "API", "TC非标", "个金", "credit", 1),
(175, "贝贝-B", "CREDIT-STAFF-BEIBEI-B", "CREDIT-STAFF-BEIBEI", "贝贝", "B", "API", "TC非标", "个金", "credit", 1),
(176, "贝贝-C", "CREDIT-STAFF-BEIBEI-C", "CREDIT-STAFF-BEIBEI", "贝贝", "C", "API", "TC非标", "个金", "credit", 1),
(177, "贝贝-D", "CREDIT-STAFF-BEIBEI-D", "CREDIT-STAFF-BEIBEI", "贝贝", "D", "API", "TC非标", "个金", "credit", 1),
(178, "人行征信-工薪-A", "RHZX-STAFF-A", "RHZX-STAFF", "人行征信-工薪", "A", "APP", "TC标准", "个金", "normal", 1),
(179, "人行征信-工薪-B", "RHZX-STAFF-B", "RHZX-STAFF", "人行征信-工薪", "B", "APP", "TC标准", "个金", "normal", 1),
(180, "人行征信-工薪-C", "RHZX-STAFF-C", "RHZX-STAFF", "人行征信-工薪", "C", "APP", "TC标准", "个金", "normal", 1),
(181, "人行征信-工薪-D", "RHZX-STAFF-D", "RHZX-STAFF", "人行征信-工薪", "D", "APP", "TC标准", "个金", "normal", 1),
(182, "公积金-工薪-A", "GJJ-STAFF-A", "GJJ-STAFF", "公积金-工薪", "A", "APP", "TC标准", "个金", "normal", 1),
(183, "公积金-工薪-B", "GJJ-STAFF-B", "GJJ-STAFF", "公积金-工薪", "B", "APP", "TC标准", "个金", "normal", 1),
(184, "公积金-工薪-C", "GJJ-STAFF-C", "GJJ-STAFF", "公积金-工薪", "C", "APP", "TC标准", "个金", "normal", 1),
(185, "公积金-工薪-D", "GJJ-STAFF-D", "GJJ-STAFF", "公积金-工薪", "D", "APP", "TC标准", "个金", "normal", 1),
(186, "奇虎360贷-A", "QIHU360-STAFF-A", "QIHU360-STAFF", "奇虎360贷", "A", "API", "TC标准", "个金", "normal", 1),
(187, "奇虎360贷-B", "QIHU360-STAFF-B", "QIHU360-STAFF", "奇虎360贷", "B", "API", "TC标准", "个金", "normal", 1),
(188, "奇虎360贷-C", "QIHU360-STAFF-C", "QIHU360-STAFF", "奇虎360贷", "C", "API", "TC标准", "个金", "normal", 1),
(189, "奇虎360贷-D", "QIHU360-STAFF-D", "QIHU360-STAFF", "奇虎360贷", "D", "API", "TC标准", "个金", "normal", 1),
(190, "展期新贷款-A", "WLD-ZQXDK-A", "WLD-ZQXDK", "展期新贷款", "A", "其他", "TC标准", "个金", "normal", 1),
(191, "展期新贷款-B", "WLD-ZQXDK-B", "WLD-ZQXDK", "展期新贷款", "B", "其他", "TC标准", "个金", "normal", 1),
(192, "H5-手机贷-A", "LOAN-LEASEBACK-H5SJD-A", "LOAN-LEASEBACK-H5SJD", "H5-手机贷", "A", "H5", "智金小额", "智金", "normal", 1),
(193, "H5-手机贷-B", "LOAN-LEASEBACK-H5SJD-B", "LOAN-LEASEBACK-H5SJD", "H5-手机贷", "B", "H5", "智金小额", "智金", "normal", 1),
(194, "H5-手机贷-C", "LOAN-LEASEBACK-H5SJD-C", "LOAN-LEASEBACK-H5SJD", "H5-手机贷", "C", "H5", "智金小额", "智金", "normal", 1),
(195, "H5-手机贷-D", "LOAN-LEASEBACK-H5SJD-D", "LOAN-LEASEBACK-H5SJD", "H5-手机贷", "D", "H5", "智金小额", "智金", "normal", 1),
(196, "我来贷大额贷-A", "LOAN-OWNER-HEXINHOUSE-A", "LOAN-OWNER-HEXINHOUSE", "我来贷大额贷", "A", "APP", "智金大额", "智金", "normal", 1),
(197, "我来贷大额贷-B", "LOAN-OWNER-HEXINHOUSE-B", "LOAN-OWNER-HEXINHOUSE", "我来贷大额贷", "B", "APP", "智金大额", "智金", "normal", 1),
(198, "我来贷大额贷-C", "LOAN-OWNER-HEXINHOUSE-C", "LOAN-OWNER-HEXINHOUSE", "我来贷大额贷", "C", "APP", "智金大额", "智金", "normal", 1),
(199, "我来贷大额贷-D", "LOAN-OWNER-HEXINHOUSE-D", "LOAN-OWNER-HEXINHOUSE", "我来贷大额贷", "D", "APP", "智金大额", "智金", "normal", 1),
(200, "场景贷-A", "CREDIT-STAFF-SCENE-A", "CREDIT-STAFF-SCENE", "场景贷", "A", "API", "TC标准", "个金", "credit", 1),
(201, "场景贷-B", "CREDIT-STAFF-SCENE-B", "CREDIT-STAFF-SCENE", "场景贷", "B", "API", "TC标准", "个金", "credit", 1),
(202, "场景贷-C", "CREDIT-STAFF-SCENE-C", "CREDIT-STAFF-SCENE", "场景贷", "C", "API", "TC标准", "个金", "credit", 1),
(203, "场景贷-D", "CREDIT-STAFF-SCENE-D", "CREDIT-STAFF-SCENE", "场景贷", "D", "API", "TC标准", "个金", "credit", 1),
(208, "返利网-A", "LOAN-STAFF-FANLIWANG-A", "LOAN-STAFF-FANLIWANG", "返利网", "A", "API", "TC标准", "个金", "normal", 1),
(209, "返利网-B", "LOAN-STAFF-FANLIWANG-B", "LOAN-STAFF-FANLIWANG", "返利网", "B", "API", "TC标准", "个金", "normal", 1),
(210, "返利网-C", "LOAN-STAFF-FANLIWANG-C", "LOAN-STAFF-FANLIWANG", "返利网", "C", "API", "TC标准", "个金", "normal", 1),
(211, "返利网-D", "LOAN-STAFF-FANLIWANG-D", "LOAN-STAFF-FANLIWANG", "返利网", "D", "API", "TC标准", "个金", "normal", 1);

COMPUTE STATS wp_man.product_info;


DROP VIEW IF EXISTS wp_calc.product_info;
CREATE VIEW wp_calc.product_info AS SELECT * FROM wp_man.product_info;
