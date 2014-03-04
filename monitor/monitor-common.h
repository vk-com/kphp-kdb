/*
    This file is part of VK/KittenPHP-DB-Engine.

    VK/KittenPHP-DB-Engine is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 2 of the License, or
    (at your option) any later version.

    VK/KittenPHP-DB-Engine is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with VK/KittenPHP-DB-Engine.  If not, see <http://www.gnu.org/licenses/>.

    This program is released under the GPL with the additional exemption 
    that compiling, linking, and/or using OpenSSL is allowed.
    You are free to remove this exemption from derived works.

    Copyright 2013 Nikolai Durov
              2013 Andrei Lopatin
*/

#ifndef __MONITOR_COMMON_H__
#define __MONITOR_COMMON_H__

#include "common-data.h"

int init_monitor (int priority);
int monitor_work (void);
int rescan_pid_table (void);

int get_monitor_pid (void);

extern int active_pnum;
extern int active_pids[CDATA_PIDS+1];

extern int am_monitor;

#define	MES_BINLOGS_MAX	1024

struct mon_binlog {
  long long binlog_name_hash;
  int binlog_data_offset;
  int mult;
};

int update_binlog_postime_info (long long binlog_name_hash, long long binlog_pos, long long binlog_time);

#endif
