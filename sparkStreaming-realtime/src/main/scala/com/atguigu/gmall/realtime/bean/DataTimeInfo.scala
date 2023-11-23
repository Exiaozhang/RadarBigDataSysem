package com.atguigu.gmall.realtime.bean

case class DataTimeInfo(
                         var data: Float,
                         var ts: Long,
                         var radartype: String,
                         var dt: String,
                         var hr: String,
                         var indexName: String,
                         var traintype: String
                       ) {
  def this() {
    this(0L, 0L, null, null, null, null, null)
  }
}

case class RcsfDataTimeInfo( //RCS数据名称
                             var rcs_name: String,
                             //雷达型号，频段，极化方式
                             var radar_type: String,
                             var frequency_band: String,
                             var polarization_mode: String,
                             //TASK
                             var target_name: String,
                             var target_specification: String,
                             var test_name: String,
                             var test_leader: String,
                             var test_unit: String,
                             var task_code: String,
                             var task_source: String,
                             var data_source: String,
                             var data_type: String,
                             var measurement_site: String,
                             var date_measurement: String,
                             var reduction_ratio: String,
                             var scale_form: String,
                             var scaler_name: String,
                             var scale_bodysize: String,
                             var uncertainty: String,
                             var scope_application: String,
                             var quality_grade: String,
                             var user_ratings: String,
                             var task_note: String,
                             //SYSTEM
                             var device_name: String,
                             var operating_system: String,
                             var dynamic_range: String,
                             var average_transmitter_power: String,
                             var system_power_stability: String,
                             var received_power_sensitivity: String,
                             var receiver_noise_coefficient: String,
                             var receiver_linearity: String,
                             var system_frequency_stability: String,
                             var system_note: String,
                             //CONDITIONS
                             var measuring_time: String,
                             var initial_operating_frequency: String,
                             var termination_frequency: String,
                             var working_frequency_step: String,
                             var polarization_combination: String,
                             var target_distance: String,
                             var target_altitude: String,
                             var antenna_height: String,
                             var target_pitch_angle: String,
                             var target_roll_angle: String,
                             var double_station_angle: String,
                             var initial_target_azimuth: String,
                             var termination_target_azimuth: String,
                             var target_azimuth_step: String,
                             var condition_note: String,
                             //DATA
                             var fo: Array[Float],
                             var rcs: Array[Float],
                             var phi: Array[Float],

                           ) {
  def this() {
    this(null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
      null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
      null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
      null, null, null, null, null)
  }
}
