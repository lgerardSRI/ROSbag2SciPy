'''
Created on Oct 12, 2015

@author: lgerard
'''
from logging import warning, info, debug
import importlib

from pathlib import Path
import h5py
import rosbag

from ros2scipy.to_numpy import parse_bag


def sanitize_dict_keys(d):
    ks = list(d.keys())
    for k in ks:
        nk = k.replace('/', '_')
        if nk != k and nk in d:
            raise ValueError("Can't sanitize")
        d[nk] = d.pop(k)




def split_base_and_head(root, rel_path):
    """ Returns the parents and the child computed from the root and the relative path.
    """
    root = root.strip('/')
    root = ('/' + root + '/') if root else '/'
    rpath = rel_path.strip('/')
    path = root + rpath
    return path.rsplit('/', 1)


def bag2h5(bag_path, db_path, db_root='/', topic_filter=None,
           custom_parser_modules=[], **kargs):
    """
    @param custom_parser_module: The name of a module containing
    a custom_parsers variable providing custom parsers.
    """

    info("Putting %s in %s with db_root=%s, topic_filter=%s, cpms=%s",
         bag_path, db_path, db_root, topic_filter, custom_parser_modules)

    # Load the custom parsers
    cp = {}
    for cpm in custom_parser_modules:
        cpm = importlib.import_module(cpm)
        cp.update(cpm.custom_parsers)

    bag_path = Path(bag_path)
    if not bag_path.exists():
        raise ValueError("The bag {} doesn't exists.".format(bag_path))

    # Load the data
    try:
        bag = rosbag.Bag(str(bag_path))
        dataset = parse_bag(bag, topic_filter=topic_filter, custom_parsers=cp)
    except Exception as e:
        debug("Raised error while parsing bag %s", bag_path, exc_info=True)
        warning(str(e))
        warning("Skipping bag %s", bag_path)
        return 1

    # Open the database
    with h5py.File(str(db_path), 'a') as db:
        cwg = db_root.rstrip('/') + '/' + bag_path.stem
        if db.get(cwg, False):
            warning("A group corresponding to {} already exists."
                    " Only new dataset entries will be added."
                    " Most probably this means that you have two bag files with the same name."
                    .format(bag_path))

        for (topic, data) in dataset.items():
            base, t = split_base_and_head(cwg, topic)
            g = db.require_group(base)
            if t in g:
                warning("The dataset {} already exists in {}, skipping it.".format(t, g))
            else:
                g.create_dataset(t, data=data)

    return 0


def checkh5bag(bag_path, db_path, db_root='/', topic_filter=set(), **kargs):
    bag_path = Path(bag_path)
    if not bag_path.exists():
        raise ValueError("The bag {} doesn't exists.".format(bag_path))

    db_path = Path(db_path)
    if not db_path.exists():
        raise ValueError("The file {} doesn't exists.".format(db_path))

    bag = rosbag.Bag(str(bag_path))
    db = h5py.File(str(db_path))

    cwg = '/' + db_root.strip('/') + '/' + bag_path.stem

    tti = bag.get_type_and_topic_info()
    topics = set(tti.topics.keys()) - set(topic_filter)

    for topic in topics:
        base, t = split_base_and_head(cwg, topic)
        g = db.get(base)
        if not g:
            raise ValueError("The group {} is missing from {} required by bag {}."
                             .format(base, db_path, bag_path))
        d = g.get(t)
        if not d:
            raise ValueError("The dataset {} is missing from group {} required by bag {}."
                             .format(t, base, bag_path))
        dlen = d.len()
        topiclen = tti.topics[topic].message_count
        if dlen != topiclen:
            raise ValueError("The dataset {}/{} is incomplete,"
                             "found {} values out of {} required by bag {}."
                             .format(base, t, dlen, topiclen, bag_path))
    info("%s is ok.", bag)


