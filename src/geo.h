#ifndef __GEO_H__
#define __GEO_H__

#include "server.h"

/* Structures used inside geo.c in order to represent points and array of
 * points on the earth. */
// 表示地理位置的结构
typedef struct geoPoint {
    // 经度
    double longitude;
    // 纬度
    double latitude;
    // 这个经纬度与另一个点之间的距离
    double dist;
    // 解码出经纬度的分值
    double score;
    // 分值对应的有序集合成员
    char *member;
} geoPoint;

// 用于储存多个地理位置的数组
typedef struct geoArray {
    // 数组本身
    struct geoPoint *array;
    // 数组可用的项数量
    size_t buckets;
    // 数组目前已用的项数量
    size_t used;
} geoArray;

#endif
