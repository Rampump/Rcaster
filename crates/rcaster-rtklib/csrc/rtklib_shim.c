#include <stdint.h>
#include "rtklib.h"

#include <stdlib.h>
#include <string.h>

typedef struct {
    rtcm_t rtcm;
} rcaster_rtklib_t;

void *rcaster_rtklib_new(void) {
    rcaster_rtklib_t *instance = (rcaster_rtklib_t *)calloc(1, sizeof(rcaster_rtklib_t));
    if (!instance) return NULL;
    if (!init_rtcm(&instance->rtcm)) {
        free(instance);
        return NULL;
    }
    return instance;
}

void rcaster_rtklib_free(void *opaque) {
    rcaster_rtklib_t *instance = (rcaster_rtklib_t *)opaque;
    if (!instance) return;
    free_rtcm(&instance->rtcm);
    free(instance);
}

const char *rcaster_rtklib_version(void) {
    return VER_RTKLIB;
}

int rcaster_rtklib_decode_rtcm3(
    void *opaque,
    const uint8_t *data,
    size_t length,
    int *types,
    int max_types
) {
    rcaster_rtklib_t *instance = (rcaster_rtklib_t *)opaque;
    int count = 0;
    size_t i;
    if (!instance || !data || (max_types > 0 && !types)) return -1;
    /* Requests are complete record batches, never continuations. */
    instance->rtcm.nbyte = 0;
    instance->rtcm.len = 0;
    instance->rtcm.word = 0;
    for (i = 0; i < length; i++) {
        int status = input_rtcm3(&instance->rtcm, data[i]);
        if (status < 0) return -2;
        if (status > 0) {
            if (count < max_types) {
                types[count] = (int)getbitu(instance->rtcm.buff, 24, 12);
            }
            count++;
        }
    }
    return count;
}
