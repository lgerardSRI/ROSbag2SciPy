'''
Created on Oct 12, 2015

@author: lgerard
'''
import importlib

from pathlib import Path
import rosbag
import tables

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

def bag2tbl(bag_path, table_path, table_root='/', topic_filter=None, custom_parser_module=None, title=""):
    """
    @param custom_parser_module: The name of a module containing a custom_parsers variable providing custom parsers.
    """
    # Load the custom parsers
    if custom_parser_module :
        cpm = importlib.import_module(custom_parser_module)
        cp = cpm.custom_parsers
    else:
        cp = {}

    bag_path = Path(bag_path)
    table_path = Path(table_path)

    # Open the database
    db = tables.open_file(str(table_path), mode='a', title=title)

    # Load the data
    bag = rosbag.Bag(str(bag_path))
    # TODO: instead of loading the data into a numpy then storing it, we should directly write in the table, esp for big files.
    dataset = parse_bag(bag, topic_filter=topic_filter, custom_parsers=cp)

    cwg = '/' + table_root.strip('/') + '/' + bag_path.stem

    for (topic, data) in dataset.items():
        base, t = split_base_and_head(cwg, topic)
        db.create_table(base, t, createparents=True, obj=data)

    db.close()


def folder2tbl(folder, **kargs):
    """
    Look for all .bag files in folder and call bag2tbl with the rest of the arguments on each one.
    """
    d = Path(folder)
    table = d.with_suffix('.h5')
    for bag in d.glob('*.bag'):
        bag2tbl(bag, table, **kargs)
