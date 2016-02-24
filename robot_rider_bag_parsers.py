'''
Created on Oct 12, 2015

@author: lgerard
'''


from ros2scipy.to_numpy import Parser_desc, add_generic_parser
import numpy as np


def add_motorstates_parser(parsers):
    """ This adds a custom parser for the robot_rider_msgs/MotorStates msg type.
    The topic has two fields, the usual header and the states field
    which is a dynamic syze array of sri_actuator_msgs/MotorState.
    We assume that this array is always of the same size and correspond to actuators:
    robot_rider_ros/robot_rider_interface/include/robot_rider_interface/hardware_interface.h:
    enum ActuatorId
    {
      STEERING_ACTUATOR = 0, THROTTLE_ACTUATOR,
      CLUTCH_ACTUATOR,       SHIFTER_ACTUATOR,
      FRONT_BRAKE_ACTUATOR,  REAR_BRAKE_ACTUATOR,
      ACTUATOR_COUNT
    };
    """
    (mp, mt) = add_generic_parser('sri_actuator_msgs/MotorState', parsers)
    (hp, ht) = add_generic_parser('std_msgs/Header', parsers)
    def parser(msg):
        return (hp(msg.header), mp(msg.states[0]), mp(msg.states[1]),
                                mp(msg.states[2]), mp(msg.states[3]),
                                mp(msg.states[4]), mp(msg.states[5]))
    msg_dtype = np.dtype([('header', ht), ('steering', mt), ('throttle', mt),
                                         ('clutch', mt), ('shifter', mt),
                                         ('brake_front', mt), ('brake_rear', mt)])
    p_desc = Parser_desc(parser, msg_dtype)
    parsers['robot_rider_msgs/MotorStates'] = p_desc
    return p_desc


custom_parsers = {}
add_motorstates_parser(custom_parsers)