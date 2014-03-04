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

    Copyright 2011-2013 Vkontakte Ltd
              2011-2013 Arseny Smirnov
              2011-2013 Aliaksei Levin
*/

#pragma once

#define TL_PHOTO_PHOTO_LOCATION 0xe75e5e39
#define TL_PHOTO_AUDIO_LOCATION 0x5dd41bef
#define TL_PHOTO_VIDEO_LOCATION 0x0dd3d270

#define TL_PHOTO_PHOTO_LOCATION_STORAGE 0xa88403da
#define TL_PHOTO_AUDIO_LOCATION_STORAGE 0x69bcb751
#define TL_PHOTO_VIDEO_LOCATION_STORAGE 0x8ae385ff

#define TL_PHOTO_PHOTO 0x2469893a
#define TL_PHOTO_AUDIO 0x7ab683e6
#define TL_PHOTO_VIDEO 0xcbb6b387

#define TL_PHOTO_PHOTO_ALBUM 0x691a0984
#define TL_PHOTO_AUDIO_ALBUM 0xc5a21f77
#define TL_PHOTO_VIDEO_ALBUM 0x7bbe3606


#define TL_PHOTO_CHANGE_PHOTO 0xaa81d661
#define TL_PHOTO_CHANGE_PHOTO_ALBUM 0xb8c9222f
#define TL_PHOTO_CHANGE_AUDIO 0x29bf54b6
#define TL_PHOTO_CHANGE_AUDIO_ALBUM 0x5fc969b5
#define TL_PHOTO_CHANGE_VIDEO 0xd26bd91a
#define TL_PHOTO_CHANGE_VIDEO_ALBUM 0x93a2903d

#define TL_PHOTO_INCREMENT_PHOTO_FIELD 0x82d012d3
#define TL_PHOTO_INCREMENT_AUDIO_FIELD 0xb0280808
#define TL_PHOTO_INCREMENT_VIDEO_FIELD 0x3b3728ae
#define TL_PHOTO_INCREMENT_ALBUM_FIELD 0x208db029

#define TL_PHOTO_SET_VOLUME_SERVER 0x536b17da

#define TL_PHOTO_DELETE_LOCATION_STORAGE 0x9196aeae
#define TL_PHOTO_DELETE_LOCATION 0x3174408a
#define TL_PHOTO_DELETE_ORIGINAL_LOCATION_STORAGE 0xff49dcbc
#define TL_PHOTO_DELETE_ORIGINAL_LOCATION 0x1cdbd0ba

#define TL_PHOTO_CHANGE_LOCATION_SERVER 0xcad8b2ad
#define TL_PHOTO_CHANGE_ORIGINAL_LOCATION_SERVER 0x852693fa
#define TL_PHOTO_CHANGE_LOCATION_SERVER2 0xe184c240
#define TL_PHOTO_CHANGE_ORIGINAL_LOCATION_SERVER2 0x14c4f836

#define TL_PHOTO_SAVE_PHOTO_LOCATION 0xb91ce359
#define TL_PHOTO_RESTORE_PHOTO_LOCATION 0x3ec57f87

#define TL_PHOTO_ROTATE_PHOTO 0x0fc8045c

#define TL_PHOTO_CHANGE_PHOTO_ORDER 0x1393c62a
#define TL_PHOTO_CHANGE_AUDIO_ORDER 0xe7c5e421
#define TL_PHOTO_CHANGE_VIDEO_ORDER 0x00b8b02e
#define TL_PHOTO_CHANGE_ALBUM_ORDER 0x1779583a

#define TL_PHOTO_NEW_PHOTO_FORCE 0x6e00fe86
#define TL_PHOTO_NEW_AUDIO_FORCE 0xcc1c5fc2
#define TL_PHOTO_NEW_VIDEO_FORCE 0xf0293f5b
#define TL_PHOTO_NEW_ALBUM_FORCE 0xa67ab146

#define TL_PHOTO_NEW_PHOTO 0x3ff3e78b
#define TL_PHOTO_NEW_AUDIO 0xbf78d33d
#define TL_PHOTO_NEW_VIDEO 0x4241e929
#define TL_PHOTO_NEW_ALBUM 0x228aa7a8

#define TL_PHOTO_GET_PHOTOS_OVERVIEW 0xf791fdde
#define TL_PHOTO_GET_PHOTOS_OVERVIEW_COUNT 0x9d9e2228

#define TL_PHOTO_GET_PHOTOS_COUNT 0xf16ea5bf
#define TL_PHOTO_GET_AUDIOS_COUNT 0x6ec072e9
#define TL_PHOTO_GET_VIDEOS_COUNT 0xadf3c251
#define TL_PHOTO_GET_ALBUMS_COUNT 0xe9e2b54c

#define TL_PHOTO_GET_PHOTOS 0xe1ff6ec3
#define TL_PHOTO_GET_PHOTOS_WITH_COUNT 0x46730412
#define TL_PHOTO_GET_AUDIOS 0xb0975096
#define TL_PHOTO_GET_AUDIOS_WITH_COUNT 0xc0afaa9e
#define TL_PHOTO_GET_VIDEOS 0xe3572ade
#define TL_PHOTO_GET_VIDEOS_WITH_COUNT 0x2ba290ec

#define TL_PHOTO_GET_PHOTO_ALBUMS 0x2a9e4291
#define TL_PHOTO_GET_PHOTO_ALBUMS_WITH_COUNT 0x3cadfa78
#define TL_PHOTO_GET_AUDIO_ALBUMS 0xfe8190c6
#define TL_PHOTO_GET_AUDIO_ALBUMS_WITH_COUNT 0xbff9f8dc
#define TL_PHOTO_GET_VIDEO_ALBUMS 0x7cfc78b7
#define TL_PHOTO_GET_VIDEO_ALBUMS_WITH_COUNT 0x99a2ea71

#define TL_PHOTO_GET_PHOTO 0xea4e08f8
#define TL_PHOTO_GET_AUDIO 0x792a6d23
#define TL_PHOTO_GET_VIDEO 0xdea33122

#define TL_PHOTO_GET_PHOTO_ALBUM 0x60d7649f
#define TL_PHOTO_GET_AUDIO_ALBUM 0xcc7ce3a3
#define TL_PHOTO_GET_VIDEO_ALBUM 0xa7a878fb

#define TL_PHOTO_RESTORE_PHOTO 0x1e8b3b25
#define TL_PHOTO_RESTORE_AUDIO 0xa073ee3d
#define TL_PHOTO_RESTORE_VIDEO 0x918fb5a7

#define TL_PHOTO_DELETE_PHOTO 0x224bf91c
#define TL_PHOTO_DELETE_AUDIO 0x9cb32c04
#define TL_PHOTO_DELETE_VIDEO 0xad4f779e
#define TL_PHOTO_DELETE_ALBUM 0xf460c6e2
