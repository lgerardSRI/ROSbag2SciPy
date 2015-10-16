'''
Created on Oct 7, 2015

@author: lgerard
'''

from collections import namedtuple

from genmsg.msgs import parse_type
from roslib.message import get_message_class

import numpy as np


def basic_types_parsing_function(x):
    """ This function is the identity since in fact the message has been already parsed.
    If we were to read the raw messages, it would be custom to each basic type.
    """
    return x

def time_parsing_function(x):
    """ Return a uint64 in nanoseconds since epoch.
    """
    return x.to_nsec()

def duration_types_parsing_function(x):
    """ Return a int64 in nanoseconds since epoch.
    """
    return x.to_nsec()

Parser_desc = namedtuple("Parser_desc", ["parser", "dtype"])

basic_types_parsers = {
    'bool'    : Parser_desc(basic_types_parsing_function, np.bool),
    'int8'    : Parser_desc(basic_types_parsing_function, np.int8),
    'int16'   : Parser_desc(basic_types_parsing_function, np.int16),
    'int32'   : Parser_desc(basic_types_parsing_function, np.int32),
    'int64'   : Parser_desc(basic_types_parsing_function, np.int64),
    'uint8'   : Parser_desc(basic_types_parsing_function, np.uint8),
    'uint16'  : Parser_desc(basic_types_parsing_function, np.uint16),
    'uint32'  : Parser_desc(basic_types_parsing_function, np.uint32),
    'uint64'  : Parser_desc(basic_types_parsing_function, np.uint64),
    'float32' : Parser_desc(basic_types_parsing_function, np.float32),
    'float64' : Parser_desc(basic_types_parsing_function, np.float64),
    # message strings are already ASCI, no conversion needed
    # TODO: document: default size to 80
    'string'  : Parser_desc(basic_types_parsing_function, np.dtype('S80')),
    'time'    : Parser_desc(time_parsing_function, np.uint64),
    'duration': Parser_desc(duration_types_parsing_function, np.int64),
    # deprecated but still used:
    'char'    : Parser_desc(basic_types_parsing_function, np.uint8),
    'byte'    : Parser_desc(basic_types_parsing_function, np.uint8),

}


def _generic_fixed_size_array_parser(base_parser_desc, array_size):
    bparser = base_parser_desc.parser
    bdtype = base_parser_desc.dtype
    def parser(msg):
        return np.fromiter((bparser(msg[i]) for i in range(array_size)), bdtype, array_size)
    return parser


def add_generic_parser(msg_typename, parsers):
    """ Adds a generic parser for msg_typename in the parsers dictionnary.
    @param parsers:
    The parsers dictionnary is expected to have entries of type Parser_desc.
    @attention:
    Any msg_typename of subfield ((recursively) which isn't in parsers
    will get a generic parser and it'll be added in parsers.
    So that custom parsers will be used for subfields only if they are defined in parsers *before*
    generating custom parsers.
    @return: the entry added in parsers for msg_typename
    """

    # First check if the parser is already defined
    if msg_typename in parsers:
        return parsers[msg_typename]

    try:
        c = get_message_class(msg_typename)
    except ValueError:
        if msg_typename in basic_types_parsers:
            # Nothing is given for this basic type, we use the generic one
            bpd = basic_types_parsers[msg_typename]
            parsers[msg_typename] = bpd
            return bpd
        else:
            raise ValueError("Trying to parse message type {} which is not in the environment."
                             " You probably need to source the correct ROS setup.bash".format(msg_typename))

    fdtypes = []
    fparsers = []

    for (field, ros_type) in zip(c.__slots__, c._slot_types):
        (rbtype, is_array, array_size) = parse_type(ros_type)
        fparser_desc = add_generic_parser(rbtype, parsers)
        if is_array:
            if array_size is None: # dynamic arrays, no support for it now
                raise ValueError("Message field {}.{} is of dynamic size, please provide a custom parser."
                                 .format(msg_typename, field))
            else: # Fixed size array
                fdtypes.append((field, fparser_desc.dtype, array_size))
                fparsers.append((field, _generic_fixed_size_array_parser(fparser_desc, array_size)))
        else:
            fparsers.append((field, fparser_desc.parser))
            fdtypes.append((field, fparser_desc.dtype))
    # TODO do actual raw parsing ?
    # TODO backport this in the genpy functions to have a fully numpy deserialization method.
    def parser(msg):
        return tuple(p(getattr(msg, f)) for (f, p) in fparsers)

    msg_dtype = np.dtype(fdtypes)
    parsers[msg_typename] = Parser_desc(parser, msg_dtype)
    return Parser_desc(parser, msg_dtype)


Topic_dataset = namedtuple("topic_dataset", ["index", "data"])

def parse_bag(bag, topic_filter=None, custom_parsers={}, basic_types_parsers=basic_types_parsers):
    tts = bag.get_type_and_topic_info()

    topics = set(tts.topics.keys())
    # Compute the intersection between the bag's topics and the topic_filter
    if topic_filter:
        topics -= set(topic_filter)

    dataset = dict()
    msgparsers = dict()
    msgidx = dict()

    parsers = dict(basic_types_parsers)
    parsers.update(custom_parsers)

    # Prepare the arrays
    for t in topics:
        size = tts.topics[t].message_count
        try:
            (parser, ddtype) = add_generic_parser(tts.topics[t].msg_type, parsers)
        except ValueError as e:
            raise ValueError("Cannot setup parser for topic {}: {}".format(t, e))
        dtype = np.dtype([('index', np.uint64), ('data', ddtype)])
        dataset[t] = np.empty(size, dtype=dtype)
        msgparsers[t] = parser
        msgidx[t] = 0

    # Go through the bag
    for (topic, msg, time) in bag.read_messages(topics=topics):
        idx = msgidx[topic]
        dataset[topic][idx][0] = time.to_nsec()
        dataset[topic][idx][1] = msgparsers[topic](msg)
        msgidx[topic] = idx + 1
    return dataset

