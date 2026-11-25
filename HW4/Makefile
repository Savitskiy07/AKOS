CC = gcc
CPPFLAGS = -Isrc -D_POSIX_C_SOURCE=200809L
CFLAGS = -std=c11 -O2 -g -Wall -Wextra -Wpedantic -Werror
LDFLAGS =
BUILD ?= build
PROGRAMS = $(BUILD)/gallery_server $(BUILD)/gallery_visitor $(BUILD)/gallery_observer

.PHONY: all clean test audit sanitize analyze
all: $(PROGRAMS)

$(BUILD):
	mkdir -p $@

$(BUILD)/gallery_%: src/%.c src/common.c src/common.h | $(BUILD)
	$(CC) $(CPPFLAGS) $(CFLAGS) src/$*.c src/common.c $(LDFLAGS) -o $@

test: all
	python3 tests/integration.py --bin $(BUILD)

audit: all
	python3 tests/audit.py --bin $(BUILD)

sanitize:
	$(MAKE) BUILD=build-sanitize CFLAGS='-std=c11 -O1 -g -Wall -Wextra -Wpedantic -Werror -fsanitize=address,undefined -fno-omit-frame-pointer -fno-pie' LDFLAGS='-fsanitize=address,undefined -no-pie' all
	ASAN_OPTIONS="$${ASAN_OPTIONS:-detect_leaks=1:halt_on_error=1}" UBSAN_OPTIONS=halt_on_error=1 python3 tests/integration.py --bin build-sanitize

analyze:
	$(MAKE) BUILD=build-analyze CFLAGS='-std=c11 -O0 -g -Wall -Wextra -Wpedantic -Werror -fanalyzer' all

clean:
	rm -rf build build-sanitize build-analyze
