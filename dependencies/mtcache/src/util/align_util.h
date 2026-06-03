#pragma once

#define ROUND_UP(val, align) ((((val) + ((align)-1)) / (align)) * (align))
