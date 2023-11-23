package com.atguigu.springboot.springbootdemo.controller;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializeConfig;
import com.atguigu.springboot.springbootdemo.bean.SnowFlake;
import com.atguigu.springboot.springbootdemo.service.CustomerService;
import com.atguigu.springboot.springbootdemo.service.impl.CustomerServiceImplNew;
import com.atujn.radar.app.DatabaseProduce;
import lombok.var;
import net.snowflake.client.jdbc.internal.google.api.services.storage.Storage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.*;
import com.alibaba.fastjson.JSON;

import java.awt.geom.Arc2D;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.sql.*;
import java.text.MessageFormat;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;


class MyFactoryUtils {

    public static CustomerService newInstance() {
        //return new CustomerServiceImpl();
        return new CustomerServiceImplNew();
    }
}

/**
 * 控制层
 */
//@Controller  // 标识为控制层（Spring）
@RestController  // @Controller + @ResponseBody
public class CustomerController {

    static Connection connection;

    public CustomerController() {
        connection = build();
    }


    /**
     * 创建Sql客户端对象
     */
    Connection build() {
        // 连接到端口号为3306的mysql数据库
        String url = "jdbc:mysql://hadoop102:3306/radar?rewriteBatched-Statements=true&useServerPrepStmts=false";
        String driver = "com.mysql.jdbc.Driver";
        String username = "root";
        String password = "000000";

        try {
            Class.forName(driver);
            connection = DriverManager.getConnection(url, username, password);
        } catch (Exception e) {
            System.out.println(e);
        }

        return connection;
    }


    //单例    接口  工厂

    //直接在类中创建对象，写死了.
    //CustomerServiceImpl customerService = new CustomerServiceImpl();

    //需求: 更换业务层的实现. CustomerServiceNew
    //CustomerServiceNew customerService = new CustomerServiceNew();

    //接口:
    //CustomerService customerService = MyFactoryUtils.newInstance();

    //SpringBoot
    @Autowired  //从Spring容器中找到对应类型的对象， 注入过来
    @Qualifier("customerServiceImplNew")  // 明确指定将哪个对象注入过来
            CustomerService customerService;


    /**
     * http://localhost:8080/login?username=zhangsan&password=123456
     */

    @GetMapping("findRadarData")
    public JSON findradarData(
            @RequestParam("indexName") String indexName,
            @RequestParam("fieldName") String fieldName
    ) throws SQLException {
        return customerService.findData(indexName, fieldName);
    }

    @GetMapping("insertRadarData")
    public String insertradarData(
            @RequestParam("indexName") String indexName,
            @RequestParam("data") Float data,
            @RequestParam("ts") long ts,
            @RequestParam("radartype") String radartype,
            @RequestParam("dt") String dt,
            @RequestParam("hr") String hr
    ) throws SQLException {

        return customerService.insertData(indexName, data, ts, radartype, dt, hr);
    }

    @ResponseBody
    @RequestMapping(value = "/json/arithmetic", method = RequestMethod.POST, produces = "application/json")
    public String computationalData(@RequestBody JSONObject jsonObject) {

        DatabaseProduce.producedata(JSON.toJSONString(jsonObject, new SerializeConfig(true)));
        return "Success";
    }

    @GetMapping("getTable")
    public JSON gettable() throws SQLException {
        return customerService.searchTable();
    }

    @GetMapping("getJson")
    public JSON getjson() {

        JSONObject json = new JSONObject();
        json.put("account", "Hello");
        json.put("password", "World");

        return json;
    }

    @GetMapping("getTestMessage")
    public JSON gettestmessag() {

        JSONObject json = new JSONObject();
        json.put("account", "Hello");
        json.put("password", "World");

        return json;
    }

    @GetMapping("deleteData")
    public String deleteData(@RequestParam("Id") String Id) {
        try {
            PreparedStatement ps;
            String tempstr;
            System.out.println("Ready Del Data");

            tempstr = "DELETE FROM RCSDATA WHERE ID=?";
            ps = connection.prepareStatement(tempstr);
            ps.setString(1, Id);
            if (ps.executeUpdate() > 0) {
                tempstr = "DELETE FROM RCSCONFIG WHERE Id=?";
                ps = connection.prepareStatement(tempstr);
                ps.setString(1, Id);
                ps.executeUpdate();
            }


            tempstr = "DELETE FROM TransData WHERE ID=?";
            ps = connection.prepareStatement(tempstr);
            ps.setString(1, Id);
            if (ps.executeUpdate() > 0) {
                tempstr = "DELETE FROM TransConfig WHERE Id=?";
                ps = connection.prepareStatement(tempstr);
                ps.setString(1, Id);
                ps.executeUpdate();
            }


            tempstr = "DELETE FROM DirectionData  WHERE ID=?";
            ps = connection.prepareStatement(tempstr);
            ps.setString(1, Id);
            if (ps.executeUpdate() > 0) {
                tempstr = "DELETE FROM DirectionConfig WHERE Id=?";
                ps = connection.prepareStatement(tempstr);
                ps.setString(1, Id);
                ps.executeUpdate();
            }

        } catch (Exception e) {
            System.out.println(e);
        }
        return null;
    }

    @GetMapping("getDataUseId")
    public String getDataUseId(@RequestParam("Id") String Id) {
        try {
            System.out.println("Ready Query Data");
            JSONObject jsonObject = new JSONObject();
            String configSql = "SELECT * FROM rcs_config WHERE id=?";
            PreparedStatement ConfigPs = connection.prepareStatement(configSql);
            ConfigPs.setString(1, Id);
            ResultSet configResultSet = ConfigPs.executeQuery();
            while (configResultSet.next()) {
                jsonObject.put("Id", configResultSet.getString(1));
                jsonObject.put("RCSName", configResultSet.getString(4));
                jsonObject.put("RadarType", configResultSet.getString(5));
                jsonObject.put("FrequencyBand", configResultSet.getString(6));
                jsonObject.put("PolarizationMode", configResultSet.getString(7));
                jsonObject.put("TargetName", configResultSet.getString(8));
                jsonObject.put("TargetSpecification", configResultSet.getString(9));
                jsonObject.put("TestName", configResultSet.getString(10));
                jsonObject.put("TestLeader", configResultSet.getString(11));
                jsonObject.put("TestUnit", configResultSet.getString(12));
                jsonObject.put("TaskCode", configResultSet.getString(13));
                jsonObject.put("TaskSource", configResultSet.getString(14));
                jsonObject.put("DataSource", configResultSet.getString(15));
                jsonObject.put("DataType", configResultSet.getString(16));
                jsonObject.put("MeasurementSite", configResultSet.getString(17));
                jsonObject.put("DateOfMeasurement", configResultSet.getString(18));
                jsonObject.put("ReductionRatio", configResultSet.getString(19));
                jsonObject.put("ScaleForm", configResultSet.getString(20));
                jsonObject.put("ScalerName", configResultSet.getString(21));
                jsonObject.put("ScaleBodySize", configResultSet.getString(22));
                jsonObject.put("Uncertainty", configResultSet.getString(23));
                jsonObject.put("ScopeOfApplication", configResultSet.getString(24));
                jsonObject.put("QualityGrade", configResultSet.getString(25));
                jsonObject.put("UserRatings", configResultSet.getString(26));
                jsonObject.put("TaskNote", configResultSet.getString(27));
                jsonObject.put("DeviceName", configResultSet.getString(28));
                jsonObject.put("OperatingSystem", configResultSet.getString(29));
                jsonObject.put("DynamicRange", configResultSet.getString(30));
                jsonObject.put("AverageTransmitterPower", configResultSet.getString(31));
                jsonObject.put("SystemPowerStability", configResultSet.getString(32));
                jsonObject.put("ReceivedPowerSensitivity", configResultSet.getString(33));
                jsonObject.put("ReceiverNoiseCoefficient", configResultSet.getString(34));
                jsonObject.put("ReceiverLinearity", configResultSet.getString(35));
                jsonObject.put("SystemFrequencyStability", configResultSet.getString(36));
                jsonObject.put("SystemNote", configResultSet.getString(37));
                jsonObject.put("MeasuringTime", configResultSet.getString(38));
                jsonObject.put("InitialOperatingFrequency", configResultSet.getString(39));
                jsonObject.put("TerminationFrequency", configResultSet.getString(40));
                jsonObject.put("WorkingFrequencyStep", configResultSet.getString(41));
                jsonObject.put("PolarizationCombination", configResultSet.getString(42));
                jsonObject.put("TargetDistance", configResultSet.getString(43));
                jsonObject.put("TargetAltitude", configResultSet.getString(44));
                jsonObject.put("AntennaHeight", configResultSet.getString(45));
                jsonObject.put("TargetPitchAngle", configResultSet.getString(46));
                jsonObject.put("TargetRollAngle", configResultSet.getString(47));
                jsonObject.put("DoubleStationAngle", configResultSet.getString(48));
                jsonObject.put("InitialTargetAzimuth", configResultSet.getString(49));
                jsonObject.put("TerminationTargetAzimuth", configResultSet.getString(50));
                jsonObject.put("TargetAzimuthStep", configResultSet.getString(51));
                jsonObject.put("ConditionNote", configResultSet.getString(52));

            }
            String dataSql = "SELECT fo,rcs,phi FROM rcs_data WHERE Id=?";
            PreparedStatement dataPs = connection.prepareStatement(dataSql);
            dataPs.setString(1, Id);
            ResultSet dataResultSet = dataPs.executeQuery();

            ArrayList<Float> foList = new ArrayList<Float>();
            ArrayList<Float> rcsList = new ArrayList<Float>();
            ArrayList<Float> phiList = new ArrayList<Float>();

            while (dataResultSet.next()) {
                foList.add(dataResultSet.getFloat(1));
                rcsList.add(dataResultSet.getFloat(2));
                phiList.add(dataResultSet.getFloat(3));
            }

            System.out.println(foList.size());

            jsonObject.put("Fo", foList);
            jsonObject.put("RCS", rcsList);
            jsonObject.put("PHI", phiList);

            System.out.println("Finish Query");
            return jsonObject.toString();
        } catch (Exception e) {
            System.out.println(e);
        }
        return new JSONObject().toString();
    }

    @GetMapping("getRcsfResults")
    public String getRcsfResults(@RequestParam("Id") String Id) {
        try {
            System.out.println("Ready Query Data");
            JSONObject jsonObject = new JSONObject();
            String configSql = "SELECT id_pr,fo FROM rcs_maxrcs_config WHERE id=?";
            PreparedStatement ConfigPs = connection.prepareStatement(configSql);
            ConfigPs.setString(1, Id);
            ResultSet configResultSet = ConfigPs.executeQuery();

            ArrayList<String> strings = new ArrayList<>();

            while (configResultSet.next()) {

                String str = new String();

                str += "每个角度Fo为" + configResultSet.getString(2) + "时,";

                String dataSql = "SELECT rcs FROM rcs_maxrcs_data WHERE id_pr=?";
                PreparedStatement dataPs = connection.prepareStatement(dataSql);
                dataPs.setString(1, configResultSet.getString(1));
                ResultSet dataResultSet = dataPs.executeQuery();
                while (dataResultSet.next()) {
                    str += "rcs最大值为" + dataResultSet.getString(1);
                }

                strings.add(str);
            }

            jsonObject.put("results", strings);
            System.out.println("Finish Query");
            return jsonObject.toString();
        } catch (Exception e) {
            System.out.println(e);
        }
        return new JSONObject().toString();
    }

    @GetMapping("getRcsfResultsIdTD")
    public String getRcsfResultsIdTD(@RequestParam("Id") String Id) {
        try {
            System.out.println("Ready Query Data");
            JSONObject jsonObject = new JSONObject();
            String configSql = "SELECT id_pr FROM rcs_test_config WHERE id=?";
            PreparedStatement ConfigPs = connection.prepareStatement(configSql);
            ConfigPs.setString(1, Id);
            ResultSet configResultSet = ConfigPs.executeQuery();

            ArrayList<String> strings = new ArrayList<>();

            while (configResultSet.next()) {

                String str = new String();
                str += configResultSet.getString(1);
                strings.add(str);
            }

            jsonObject.put("results", strings);
            System.out.println("Finish Query");
            return jsonObject.toString();
        } catch (Exception e) {
            System.out.println(e);
        }
        return new JSONObject().toString();
    }

    @GetMapping("getRcsfResultsTD")
    public String getRcsfResultsTD(@RequestParam("Id") String Id) {
        try {

            System.out.println(Id);

            JSONObject jsonObject = new JSONObject();
            String configSql = "SELECT x,y FROM rcs_test_data WHERE id_pr=?";
            PreparedStatement ConfigPs = connection.prepareStatement(configSql);
            ConfigPs.setString(1, Id);
            ResultSet configResultSet = ConfigPs.executeQuery();

            ArrayList<Float> xData = new ArrayList<>();
            ArrayList<Float> yData = new ArrayList<>();

            while (configResultSet.next()) {
                Float x = configResultSet.getFloat(1);
                Float y = configResultSet.getFloat(2);
                xData.add(x);
                yData.add(y);
                System.out.println(x + "|" + y);
            }

            jsonObject.put("xData", xData);
            jsonObject.put("yData", yData);
            System.out.println("Finish Query");
            return jsonObject.toString();
        } catch (Exception e) {
            System.out.println(e);
        }
        return new JSONObject().toString();
    }

    @GetMapping("getDataDirection")
    public String getDataDirection(@RequestParam("Id") String Id) {
        try {
            System.out.println("Ready Query Data");
            JSONObject jsonObject = new JSONObject();

            String dataSql = "SELECT angle,level,phase FROM direction_data WHERE Id=?";
            PreparedStatement dataPs = connection.prepareStatement(dataSql);
            dataPs.setString(1, Id);
            ResultSet dataResultSet = dataPs.executeQuery();

            ArrayList<Float> angleList = new ArrayList<Float>();
            ArrayList<Float> leveList = new ArrayList<Float>();
            ArrayList<Float> phaseList = new ArrayList<Float>();

            while (dataResultSet.next()) {
                angleList.add(dataResultSet.getFloat(1));
                leveList.add(dataResultSet.getFloat(2));
                phaseList.add(dataResultSet.getFloat(3));
            }

            jsonObject.put("Angle", angleList);
            jsonObject.put("Level", leveList);
            jsonObject.put("Phase", phaseList);

            System.out.println("Finish Query");
            return jsonObject.toString();
        } catch (Exception e) {
            System.out.println(e);
        }
        return new JSONObject().toString();
    }

    @GetMapping("getDataTrans")
    public String getDataTrans(@RequestParam("Id") String Id) {
        try {
            System.out.println("Ready Query Data");
            JSONObject jsonObject = new JSONObject();
            String dataSql = "SELECT azt,azimuth_angle,angle_pitch,level_value FROM trans_data WHERE id=?";
            PreparedStatement dataPs = connection.prepareStatement(dataSql);
            dataPs.setString(1, Id);
            ResultSet dataResultSet = dataPs.executeQuery();

            ArrayList<Float> aztList = new ArrayList<Float>();
            ArrayList<Float> azimuthAngleList = new ArrayList<Float>();
            ArrayList<Float> angleOfPitchList = new ArrayList<Float>();
            ArrayList<Float> levelValueList = new ArrayList<Float>();

            while (dataResultSet.next()) {
                aztList.add(dataResultSet.getFloat(1));
                azimuthAngleList.add(dataResultSet.getFloat(2));
                angleOfPitchList.add(dataResultSet.getFloat(3));
                levelValueList.add(dataResultSet.getFloat(4));
            }

            jsonObject.put("Azt", aztList);
            jsonObject.put("AzimuthAngle", azimuthAngleList);
            jsonObject.put("AngleOfPitch", angleOfPitchList);
            jsonObject.put("LevelValue", levelValueList);

            System.out.println("Finish Query");
            return jsonObject.toString();
        } catch (Exception e) {
            System.out.println(e);
        }
        return new JSONObject().toString();
    }

    @ResponseBody
    @RequestMapping(value = "/json/data", method = RequestMethod.POST, produces = "application/json")
    public String getByJSON(@RequestBody JSONObject jsonObject) {
        try {
            // 直接将json信息打印出来
            //System.out.println(jsonObject.toJSONString());
            SnowFlake snowFlake = new SnowFlake(1, 1);
            Long id = snowFlake.nextId();

            LocalDateTime now = LocalDateTime.now();
            Timestamp timestamp = java.sql.Timestamp.valueOf(now);
            String sqlTime = timestamp.toString();

            String sql = "INSERT INTO rcs_config (radar_type, frequency_band, polarization_mode, target_name, target_specification, test_name, test_leader, test_unit, " +
                    "task_code, task_source, data_source, data_type, measurement_site, date_measurement, reduction_ratio, scale_form, scaler_name, scale_bodysize, " +
                    "uncertainty, scope_application, quality_grade, user_ratings, task_note, device_name, operating_system, dynamic_range, average_transmitter_power," +
                    " system_power_stability, received_power_sensitivity, receiver_noise_coefficient, receiver_linearity, system_frequency_stability, system_note," +
                    " measuring_time, initial_operating_frequency,termination_frequency ,working_frequency_step ,polarization_combination ,target_distance ,target_altitude" +
                    " ,antenna_height ,target_pitch_angle ,target_roll_angle ,double_station_angle ,initial_target_azimuth ,termination_target_azimuth ,target_azimuth_step ,condition_note,id,rcs_name,gmt_create) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
            PreparedStatement ps = connection.prepareStatement(sql);
            ps.setString(1, jsonObject.get("RadarType").toString());
            ps.setString(2, jsonObject.get("FrequencyBand").toString());
            ps.setString(3, jsonObject.get("PolarizationMode").toString());
            ps.setString(4, jsonObject.get("TargetName").toString());
            ps.setString(5, jsonObject.get("TargetSpecification").toString());
            ps.setString(6, jsonObject.get("TestName").toString());
            ps.setString(7, jsonObject.get("TestLeader").toString());
            ps.setString(8, jsonObject.get("TestUnit").toString());
            ps.setString(9, jsonObject.get("TaskCode").toString());
            ps.setString(10, jsonObject.get("TaskSource").toString());
            ps.setString(11, jsonObject.get("DataSource").toString());
            ps.setString(12, jsonObject.get("DataType").toString());
            ps.setString(13, jsonObject.get("MeasurementSite").toString());
            ps.setString(14, jsonObject.get("DateOfMeasurement").toString());
            ps.setString(15, jsonObject.get("ReductionRatio").toString());
            ps.setString(16, jsonObject.get("ScaleForm").toString());
            ps.setString(17, jsonObject.get("ScalerName").toString());
            ps.setString(18, jsonObject.get("ScaleBodySize").toString());
            ps.setString(19, jsonObject.get("Uncertainty").toString());
            ps.setString(20, jsonObject.get("ScopeOfApplication").toString());
            ps.setString(21, jsonObject.get("QualityGrade").toString());
            ps.setString(22, jsonObject.get("UserRatings").toString());
            ps.setString(23, jsonObject.get("TaskNote").toString());
            ps.setString(24, jsonObject.get("DeviceName").toString());
            ps.setString(25, jsonObject.get("OperatingSystem").toString());
            ps.setString(26, jsonObject.get("DynamicRange").toString());
            ps.setString(27, jsonObject.get("AverageTransmitterPower").toString());
            ps.setString(28, jsonObject.get("SystemPowerStability").toString());
            ps.setString(29, jsonObject.get("ReceivedPowerSensitivity").toString());
            ps.setString(30, jsonObject.get("ReceiverNoiseCoefficient").toString());
            ps.setString(31, jsonObject.get("ReceiverLinearity").toString());
            ps.setString(32, jsonObject.get("SystemFrequencyStability").toString());
            ps.setString(33, jsonObject.get("SystemNote").toString());
            ps.setString(34, jsonObject.get("MeasuringTime").toString());
            ps.setString(35, jsonObject.get("InitialOperatingFrequency").toString());
            ps.setString(36, jsonObject.get("TerminationFrequency").toString());
            ps.setString(37, jsonObject.get("WorkingFrequencyStep").toString());
            ps.setString(38, jsonObject.get("PolarizationCombination").toString());
            ps.setString(39, jsonObject.get("TargetDistance").toString());
            ps.setString(40, jsonObject.get("TargetAltitude").toString());
            ps.setString(41, jsonObject.get("AntennaHeight").toString());
            ps.setString(42, jsonObject.get("TargetPitchAngle").toString());
            ps.setString(43, jsonObject.get("TargetRollAngle").toString());
            ps.setString(44, jsonObject.get("DoubleStationAngle").toString());
            ps.setString(45, jsonObject.get("InitialTargetAzimuth").toString());
            ps.setString(46, jsonObject.get("TerminationTargetAzimuth").toString());
            ps.setString(47, jsonObject.get("TargetAzimuthStep").toString());
            ps.setString(48, jsonObject.get("ConditionNote").toString());
            ps.setString(49, Long.toString(id));
            ps.setString(50, jsonObject.get("RCSName").toString());
            ps.setString(51, sqlTime);
            ps.executeUpdate();

            JSONArray foJsonArray = jsonObject.getJSONArray("Fo");
            JSONArray rcsJsonArray = jsonObject.getJSONArray("RCS");
            JSONArray phiJsonArray = jsonObject.getJSONArray("PHI");

            ArrayList<Float> foList = new ArrayList<Float>();
            ArrayList<Float> rcsList = new ArrayList<Float>();
            ArrayList<Float> phiList = new ArrayList<Float>();

            for (int i = 0; i < foJsonArray.size(); i++) {
                foList.add(foJsonArray.getFloat(i));
            }
            for (int i = 0; i < rcsJsonArray.size(); i++) {
                rcsList.add(rcsJsonArray.getFloat(i));
            }
            for (int i = 0; i < phiJsonArray.size(); i++) {
                phiList.add(phiJsonArray.getFloat(i));
            }

            String mysql = "INSERT INTO rcs_data (id, fo, rcs, phi, gmt_create) VALUES (?, ?, ?, ?, ?)";
            PreparedStatement preparedStatement = connection.prepareStatement(mysql);
            for (int i = 0; i < foJsonArray.size(); i++) {
                preparedStatement.setString(1, Long.toString(id));
                preparedStatement.setString(2, Float.toString(foList.get(i)));
                preparedStatement.setString(3, Float.toString(rcsList.get(i)));
                preparedStatement.setString(4, Float.toString(phiList.get(i)));
                preparedStatement.setString(5, sqlTime);
                preparedStatement.executeUpdate();
            }

        } catch (Exception e) {
            System.out.println(e);
        }
        // 将获取的json数据封装一层，然后在给返回
        JSONObject result = new JSONObject();
        result.put("msg", "ok");
        result.put("method", "json");
        return result.toJSONString();
    }

    @ResponseBody
    @RequestMapping(value = "/json/DirectionData", method = RequestMethod.POST, produces = "application/json")
    public String getDirectionByJson(@RequestBody JSONObject jsonObject) {
        try {

            SnowFlake snowFlake = new SnowFlake(1, 1);
            Long id = snowFlake.nextId();

            LocalDateTime now = LocalDateTime.now();
            Timestamp timestamp = java.sql.Timestamp.valueOf(now);
            String sqlTime = timestamp.toString();

            String sql = "INSERT INTO direction_config (id,gmt_create,direction_name) VALUES (?, ?, ?)";
            PreparedStatement ps = connection.prepareStatement(sql);
            ps.setString(1, Long.toString(id));
            ps.setString(2, sqlTime);
            ps.setString(3, jsonObject.getString("DirectionName"));
            ps.executeUpdate();


            JSONArray angleJsonArray = jsonObject.getJSONArray("Angle");
            JSONArray levelJsonArray = jsonObject.getJSONArray("Level");
            JSONArray phaseJsonArray = jsonObject.getJSONArray("Phase");

            ArrayList<Float> angleList = new ArrayList<Float>();
            ArrayList<Float> levelList = new ArrayList<Float>();
            ArrayList<Float> phaseList = new ArrayList<Float>();

            for (int i = 0; i < angleJsonArray.size(); i++) {
                angleList.add(angleJsonArray.getFloat(i));
            }
            for (int i = 0; i < levelJsonArray.size(); i++) {
                levelList.add(levelJsonArray.getFloat(i));
            }
            for (int i = 0; i < phaseJsonArray.size(); i++) {
                phaseList.add(phaseJsonArray.getFloat(i));
            }

            String mysql = "INSERT INTO direction_data (id,gmt_create, angle, level, phase) VALUES (?, ?, ?, ?, ?)";
            PreparedStatement preparedStatement = connection.prepareStatement(mysql);

            for (int i = 0; i < angleJsonArray.size(); i++) {
                preparedStatement.setString(1, Long.toString(id));
                preparedStatement.setString(2, sqlTime);
                preparedStatement.setString(3, Float.toString(angleList.get(i)));
                preparedStatement.setString(4, Float.toString(levelList.get(i)));
                preparedStatement.setString(5, Float.toString(phaseList.get(i)));
                preparedStatement.executeUpdate();
            }
        } catch (Exception e) {
            System.out.println(e);
        }

        JSONObject result = new JSONObject();
        result.put("msg", "ok");
        result.put("method", "json");
        return result.toJSONString();
    }

    @ResponseBody
    @RequestMapping(value = "/json/TransData", method = RequestMethod.POST, produces = "application/json")
    public String getTransByJson(@RequestBody JSONObject jsonObject) {
        try {

            SnowFlake snowFlake = new SnowFlake(1, 1);
            Long id = snowFlake.nextId();

            LocalDateTime now = LocalDateTime.now();
            Timestamp timestamp = java.sql.Timestamp.valueOf(now);
            String sqlTime = timestamp.toString();

            String sql = "INSERT INTO trans_config (id,gmt_create,transfile_name) VALUES (?, ?, ?)";
            PreparedStatement ps = connection.prepareStatement(sql);
            ps.setString(1, Long.toString(id));
            ps.setString(2, sqlTime);
            ps.setString(3, jsonObject.getString("TransName"));
            ps.executeUpdate();


            JSONArray aztJsonArray = jsonObject.getJSONArray("Azt");
            JSONArray azimuthAngleJsonArray = jsonObject.getJSONArray("AzimuthAngle");
            JSONArray angleOfPitchJsonArray = jsonObject.getJSONArray("AngleOfPitch");
            JSONArray levelValueJsonArray = jsonObject.getJSONArray("LevelValue");

            ArrayList<Float> aztList = new ArrayList<Float>();
            ArrayList<Float> azimuthAngleList = new ArrayList<Float>();
            ArrayList<Float> angleOfPitchList = new ArrayList<Float>();
            ArrayList<Float> levelValueList = new ArrayList<Float>();

            for (int i = 0; i < aztJsonArray.size(); i++) {
                aztList.add(aztJsonArray.getFloat(i));
            }
            for (int i = 0; i < azimuthAngleJsonArray.size(); i++) {
                azimuthAngleList.add(azimuthAngleJsonArray.getFloat(i));
            }
            for (int i = 0; i < angleOfPitchJsonArray.size(); i++) {
                angleOfPitchList.add(angleOfPitchJsonArray.getFloat(i));
            }
            for (int i = 0; i < levelValueJsonArray.size(); i++) {
                levelValueList.add(levelValueJsonArray.getFloat(i));
            }

            String mysql = "INSERT INTO trans_data ( id, gmt_create, azt, azimuth_angle, angle_pitch, level_value) VALUES (?, ?, ?, ?, ? ,?)";
            PreparedStatement preparedStatement = connection.prepareStatement(mysql);

            for (int i = 0; i < aztJsonArray.size(); i++) {
                preparedStatement.setString(1, Long.toString(id));
                preparedStatement.setString(2, sqlTime);
                preparedStatement.setString(3, Float.toString(aztList.get(i)));
                preparedStatement.setString(4, Float.toString(azimuthAngleList.get(i)));
                preparedStatement.setString(5, Float.toString(angleOfPitchList.get(i)));
                preparedStatement.setString(6, Float.toString(levelValueList.get(i)));
                preparedStatement.executeUpdate();
            }

        } catch (Exception e) {
            System.out.println(e);
        }

        JSONObject result = new JSONObject();
        result.put("msg", "ok");
        result.put("method", "json");
        return result.toJSONString();
    }

    @GetMapping("getAllDataName")
    public JSON getAllDataName() {
        try {
            JSONObject jsonObject = new JSONObject();
            String sql = "SELECT rcs_name,id,radar_type,gmt_create FROM rcs_config";
            PreparedStatement ps = connection.prepareStatement(sql);


            ArrayList<String> RCSNames = new ArrayList<String>();
            ArrayList<String> RCSId = new ArrayList<String>();
            ArrayList<String> RCSDateTime = new ArrayList<>();
            ArrayList<String> RCSRadarType = new ArrayList<>();
            ResultSet resultSet = ps.executeQuery();
            while (resultSet.next()) {
                String RCSName = resultSet.getString("rcs_name");
                String id = resultSet.getString("id");
                String radarType = resultSet.getString("radar_type");
                String date = resultSet.getString("gmt_create");
                RCSNames.add(RCSName);
                RCSId.add(id);
                RCSRadarType.add(radarType);
                RCSDateTime.add(date);
            }
            jsonObject.put("RCSNAMES", RCSNames);
            jsonObject.put("RCSID", RCSId);
            jsonObject.put("RcsDateTime", RCSDateTime);
            jsonObject.put("RCSRadarType", RCSRadarType);

            ArrayList<String> TransNames = new ArrayList<String>();
            ArrayList<String> TransId = new ArrayList<String>();
            ArrayList<String> TransDateTime = new ArrayList<>();
            ArrayList<String> TransRadarType = new ArrayList<>();
            sql = "SELECT transfile_name,id,radar_type,gmt_create FROM trans_config";
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                String TransName = resultSet.getString("transfile_name");
                String id = resultSet.getString("id");
                String radarType = resultSet.getString("radar_type");
                String date = resultSet.getString("gmt_create");
                TransNames.add(TransName);
                TransId.add(id);
                TransRadarType.add(radarType);
                TransDateTime.add(date);
            }
            jsonObject.put("TransNames", TransNames);
            jsonObject.put("TransId", TransId);
            jsonObject.put("TransDateTime", TransDateTime);
            jsonObject.put("TransRadarType", TransRadarType);

            ArrayList<String> DirectionNames = new ArrayList<String>();
            ArrayList<String> DirectionId = new ArrayList<String>();
            ArrayList<String> DirectionDateTime = new ArrayList<>();
            ArrayList<String> DirectionRadarType = new ArrayList<>();
            sql = "SELECT direction_name,id,radar_type,gmt_create FROM direction_config";
            PreparedStatement preparedStatement1 = connection.prepareStatement(sql);
            resultSet = preparedStatement1.executeQuery();
            while (resultSet.next()) {
                String directionName = resultSet.getString("direction_name");
                String id = resultSet.getString("id");
                String radarType = resultSet.getString("radar_type");
                String date = resultSet.getString("gmt_create");
                DirectionNames.add(directionName);
                DirectionId.add(id);
                DirectionDateTime.add(date);
                DirectionRadarType.add(radarType);
            }
            jsonObject.put("DirectionNames", DirectionNames);
            jsonObject.put("DirectionId", DirectionId);
            jsonObject.put("DirectionDateTime", DirectionDateTime);
            jsonObject.put("DirectionRadarType", DirectionRadarType);


            return jsonObject;
        } catch (Exception e) {
            System.out.println(e);
        }
        return null;
    }

    @GetMapping("getDataByConfig")
    public JSON getDataByConfig(@RequestParam("indexName") String indexName,
                                @RequestParam("options") String options) {
        try {
            JSONObject jsonObject = new JSONObject();
            String sql;
            if (options != "") {
                sql = "SELECT rcs_name,id,gmt_create FROM rcs_config WHERE " + options;
            } else {
                sql = "SELECT rcs_name,id,gmt_create FROM rcs_config";
            }

            System.out.println(sql);

            PreparedStatement ps = connection.prepareStatement(sql);


            ArrayList<String> RCSNames = new ArrayList<String>();
            ArrayList<String> RCSId = new ArrayList<String>();
            ArrayList<String> RCSDateTime = new ArrayList<>();
            ResultSet resultSet = ps.executeQuery();
            while (resultSet.next()) {
                String RCSName = resultSet.getString("rcs_name");
                String id = resultSet.getString("id");
                String date = resultSet.getString("gmt_create");
                RCSNames.add(RCSName);
                RCSId.add(id);
                RCSDateTime.add(date);
            }
            jsonObject.put("RCSNAMES", RCSNames);
            jsonObject.put("RCSID", RCSId);
            jsonObject.put("RcsDateTime", RCSDateTime);

            System.out.println(jsonObject.toString());
            return jsonObject;
        } catch (Exception e) {
            System.out.println(e);
        }
        System.out.println("wrong");
        return null;
    }

    @GetMapping("updateArithmetic")
    public String updateArithmetic() {
        String radarPath = System.getenv("RADAR_HOME");
        String fileName = radarPath + "/Algorithm.json";
        String result = "";

        try (BufferedReader reader = new BufferedReader(new FileReader(fileName))) {
            String line;
            while ((line = reader.readLine()) != null) {
                result += line + "\r\n";
            }
            return result;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }


    //通用上传数据
    @ResponseBody
    @RequestMapping(value = "/json/UpdateRadarData", method = RequestMethod.POST, produces = "application/json")
    public String UpdateRadarData(@RequestParam("dataType") String dataType, @RequestBody JSONObject jsonObject) {

        //配置和数据表名称
        String configTable = dataType + "_config";
        String dataTable = dataType + "_data";

        LocalDateTime now = LocalDateTime.now();
        Timestamp timestamp = Timestamp.valueOf(now);
        String sqlTime = timestamp.toString();

        JSONObject configObj = jsonObject.getJSONObject("config");
        JSONObject dataObj = jsonObject.getJSONObject("data");

        //构建雪花id
        SnowFlake snowFlake = new SnowFlake(1, 1);
        long id = snowFlake.nextId();

        //插入数据 id 日期
        InsertValue(configTable, "id_pr", Long.toString(id));
        UpdateValue(configTable,"gmt_create",sqlTime,"id_pr",Long.toString(id));

        //插入配置文件
        Set<String> jsonKeys = configObj.keySet();
        configObj.keySet().forEach(key -> {
            String value = configObj.getString(key);
            UpdateValue(configTable,key,value.toString(),"id_pr",Long.toString(id));
        });

        //数据文件的名称和数据
        List<String> arrarysNames = new ArrayList<String>();
        ArrayList<JSONArray> arraysData = new ArrayList<>();

        //读取Json的Key插入数据
        dataObj.keySet().forEach(key -> {
            JSONArray value = dataObj.getJSONArray(key);
            arraysData.add(value);
            arrarysNames.add(key);
        });

        int dataSize = arraysData.get(0).size();
        int dataCount = arraysData.size();

        for (int i = 0; i < dataSize - 1; i++) {
            String dataFieldNames = "id,gmt_create,";
            String dataFieldValues = id + ",'" + sqlTime + "',";

            for (int p = 0; p < dataCount - 1; p++) {
                if (p == dataSize - 1) {
                    dataFieldNames = dataFieldNames + arrarysNames.get(p);
                    dataFieldValues = dataFieldValues + arraysData.get(p).get(i);
                } else {
                    dataFieldNames = dataFieldNames + arrarysNames.get(p) + ",";
                    dataFieldValues = dataFieldValues + arraysData.get(p).get(i) + ",";
                }
            }

            InsertValue(dataTable,dataFieldNames,dataFieldValues);
        }

        return "";
    }

    //向表中插入数据
    void InsertValue(String indexName, String fieldName, String filedValue) {
        try {
            String sql = MessageFormat.format("INSERT INTO {0} ({1}) VALUES ({2})", indexName, fieldName, filedValue);
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            preparedStatement.execute();
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    //更新表的数据
    void UpdateValue(String indexName, String fieldName, String fieldValue, String conditionName, String conditionValue) {
        try {
            String sql = MessageFormat.format("UPDATE {0} SET {1} = {2} WHERE {3} = {4}", indexName, fieldName, fieldValue, conditionName, conditionValue);
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            preparedStatement.execute();
        } catch (Exception e) {
            System.out.println(e);
        }

    }
}
