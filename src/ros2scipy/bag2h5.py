'''
Created on Oct 12, 2015

@author: lgerard
'''
from logging import error, warning, debug, info
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


def _collect_folder_bags(folder, recursive=False):
    pattern = '**/*.bag' if recursive else '*.bag'
    return Path(folder).glob(pattern)


def bag2h5(bag_path, db_path, db_root='/', topic_filter=None,
           custom_parser_modules=[], **kargs):
    """
    @param custom_parser_module: The name of a module containing
    a custom_parsers variable providing custom parsers.
    """
    # Load the custom parsers
    cp = {}
    for cpm in custom_parser_modules:
        cpm = importlib.import_module(cpm)
        cp.update(cpm.custom_parsers)

    bag_path = Path(bag_path)
    if not bag_path.exists():
        raise ValueError("The bag {} doesn't exists.".format(bag_path))

    # Open the database
    db = h5py.File(str(db_path), 'a')

    # Load the data
    bag = rosbag.Bag(str(bag_path))
    dataset = parse_bag(bag, topic_filter=topic_filter, custom_parsers=cp)

    cwg = db_root.rstrip('/') + '/' + bag_path.stem

    for (topic, data) in dataset.items():
        base, t = split_base_and_head(cwg, topic)
        g = db.require_group(base)
        if t in g:
            warning("The dataset {} already exists in group {}, skipping it.".format(t, g))
        else:
            g.create_dataset(t, data=data)

    db.close()


def folder2h5(folder, db, recursive=False, **kargs):
    """
    Look for all .bag files in folder and call bag2tbl
    with the rest of the arguments on each one.
    """
    for bag in _collect_folder_bags(folder, recursive):
        bag2h5(bag, db, **kargs)


def checkh5bag(bag_path, db_path, db_root='/', topic_filter=set(), **kargs):
    bag_path = Path(bag_path)
    if not bag_path.exists():
        raise ValueError("The bag {} doesn't exists.".format(bag_path))

    db_path = Path(db_path)
    if not db_path.exists():
        raise ValueError("The file {} doesn't exists.".format(bag_path))

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

def checkfolder2h5(folder, db, recursive=False, **kargs):
    for bag in _collect_folder_bags(folder, recursive):
        try:
            checkh5bag(bag, db, **kargs)
            info("{} is ok.".format(bag))
        except ValueError as e:
            error("{} is bad: {}".format(bag, e.msg))






