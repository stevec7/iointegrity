#!/usr/bin/env python
from iointegrity.iotools import FileTree
from IPython import embed

ft = FileTree('/tmp/root', num_top_dirs=10, num_sub_dirs=10,
        third_level_dirs=3, files_per_dir=5, max_size=8192,
        suffix='.testdirs')
ft._gen_dir_array()
ft.max_size = (1024 * 1000000)
ft.aligned = False
ft.log = 'verbose'
ft.serial_create_dir_tree()
embed()

