/*
    This file is part of VK/KittenPHP-DB-Engine Library.

    VK/KittenPHP-DB-Engine Library is free software: you can redistribute it and/or modify
    it under the terms of the GNU Lesser General Public License as published by
    the Free Software Foundation, either version 2 of the License, or
    (at your option) any later version.

    VK/KittenPHP-DB-Engine Library is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Lesser General Public License for more details.

    You should have received a copy of the GNU Lesser General Public License
    along with VK/KittenPHP-DB-Engine Library.  If not, see <http://www.gnu.org/licenses/>.

    Copyright 2013 Vkontakte Ltd
              2013 Vitaliy Valtman              
              2013 Anton Maydell
*/



#include "net-connections.h"

typedef struct {
  void (*cron) (void);
  void (*sigusr1) (void);
  int  (*save_index) (int);
  void (*sighup) (void);
} server_functions_t;

typedef struct {
  int sfd;
  struct in_addr settings_addr;
} engine_t;

void engine_init (engine_t *E, const char *const pwd_filename, int index_mode);
void server_init (engine_t *E, server_functions_t *F, conn_type_t *listen_connection_type, void *listen_connection_extra);
void server_exit (engine_t *E);
int process_signals (void);
