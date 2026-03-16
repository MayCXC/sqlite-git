/*
 * ODB helper: delegate object storage to an external process.
 *
 * Follows the same pattern as transport-helper.c for remote helpers.
 * Git discovers "git-odb-<name>" in PATH, spawns it, communicates
 * via stdin/stdout pipes.
 *
 * Helper protocol:
 *   -> capabilities\n
 *   <- get\n
 *   <- put\n
 *   <- have\n
 *   <- list\n
 *   <- \n
 *
 *   -> have <oid>\n
 *   <- true\n | false\n
 *
 *   -> get <oid>\n
 *   <- <type> <size>\n
 *   <- <size bytes of data>
 *   (or: <- missing\n)
 *
 *   -> put <type> <size>\n
 *   -> <size bytes of data>
 *   <- <oid>\n
 *
 *   -> list\n
 *   <- <oid> <type> <size>\n  (repeated)
 *   <- \n
 */

#include "git-compat-util.h"
#include "hash.h"
#include "hex.h"
#include "object.h"
#include "object-file.h"
#include "odb.h"
#include "odb/source.h"
#include "repository.h"
#include "run-command.h"
#include "strbuf.h"
#include "strvec.h"

struct odb_source_helper {
	struct odb_source base;
	struct child_process *helper;
	FILE *out;  /* helper's stdout, for reading lines */
	char *helper_name;
};

static struct child_process *get_helper(struct odb_source_helper *src)
{
	struct child_process *helper;
	int duped;

	if (src->helper)
		return src->helper;

	helper = xmalloc(sizeof(*helper));
	child_process_init(helper);
	helper->in = -1;
	helper->out = -1;
	helper->err = 0;
	strvec_pushf(&helper->args, "git-odb-%s", src->helper_name);
	strvec_push(&helper->args, src->base.path);
	helper->silent_exec_failure = 1;

	if (start_command(helper) < 0)
		die("unable to start odb helper '%s'", src->helper_name);

	src->helper = helper;

	duped = dup(helper->out);
	if (duped < 0)
		die_errno("can't dup helper output fd");
	src->out = fdopen(duped, "r");
	if (!src->out)
		die_errno("can't fdopen helper output");

	/* Read capabilities */
	struct strbuf line = STRBUF_INIT;
	write_in_full(helper->in, "capabilities\n", 13);
	while (strbuf_getline_lf(&line, src->out) != EOF) {
		if (!line.len)
			break;
		/* We could parse capabilities here */
	}
	strbuf_release(&line);

	return helper;
}

static void helper_send(struct odb_source_helper *src, const char *fmt, ...)
{
	struct child_process *h = get_helper(src);
	va_list ap;
	struct strbuf buf = STRBUF_INIT;
	va_start(ap, fmt);
	strbuf_vaddf(&buf, fmt, ap);
	va_end(ap);
	write_in_full(h->in, buf.buf, buf.len);
	strbuf_release(&buf);
}

static int helper_readline(struct odb_source_helper *src, struct strbuf *line)
{
	get_helper(src);
	strbuf_reset(line);
	return strbuf_getline_lf(line, src->out);
}

/* ---- odb_source callbacks ---- */

static void helper_source_free(struct odb_source *source)
{
	struct odb_source_helper *src = (struct odb_source_helper *)source;
	if (src->helper) {
		close(src->helper->in);
		fclose(src->out);
		finish_command(src->helper);
		free(src->helper);
	}
	free(src->helper_name);
	odb_source_release(&src->base);
	free(src);
}

static void helper_source_close(struct odb_source *source)
{
	/* Keep helper alive for reuse */
}

static void helper_source_reprepare(struct odb_source *source UNUSED)
{
}

static int helper_source_read_object_info(struct odb_source *source,
					  const struct object_id *oid,
					  struct object_info *oi,
					  enum object_info_flags flags UNUSED)
{
	struct odb_source_helper *src = (struct odb_source_helper *)source;
	char hex[GIT_MAX_HEXSZ + 1];
	oid_to_hex_r(hex, oid);

	helper_send(src, "get %s\n", hex);

	struct strbuf line = STRBUF_INIT;
	if (helper_readline(src, &line) == EOF) {
		strbuf_release(&line);
		return -1;
	}

	if (!strcmp(line.buf, "missing")) {
		strbuf_release(&line);
		return -1;
	}

	/* Parse "<type> <size>" */
	char type_str[32];
	unsigned long size;
	if (sscanf(line.buf, "%31s %lu", type_str, &size) != 2) {
		strbuf_release(&line);
		return -1;
	}

	enum object_type type = type_from_string(type_str);
	if (oi->typep) *(oi->typep) = type;
	if (oi->sizep) *(oi->sizep) = size;

	if (oi->contentp) {
		*oi->contentp = xmalloc(size);
		if (read_in_full(fileno(src->out), *oi->contentp, size) != size) {
			free(*oi->contentp);
			*oi->contentp = NULL;
			strbuf_release(&line);
			return -1;
		}
	} else {
		/* Discard the data */
		char discard[4096];
		unsigned long remaining = size;
		while (remaining > 0) {
			size_t chunk = remaining < sizeof(discard) ? remaining : sizeof(discard);
			read_in_full(fileno(src->out), discard, chunk);
			remaining -= chunk;
		}
	}

	strbuf_release(&line);
	return 0;
}

static int helper_source_read_object_stream(struct odb_read_stream **out UNUSED,
					    struct odb_source *source UNUSED,
					    const struct object_id *oid UNUSED)
{
	return -1;
}

static int helper_source_for_each_object(struct odb_source *source,
					 const struct object_info *request,
					 odb_for_each_object_cb cb,
					 void *cb_data,
					 unsigned flags UNUSED)
{
	struct odb_source_helper *src = (struct odb_source_helper *)source;
	helper_send(src, "list\n");

	struct strbuf line = STRBUF_INIT;
	while (helper_readline(src, &line) != EOF) {
		if (!line.len)
			break;

		/* Parse "<oid> <type> <size>" */
		char oid_hex[GIT_MAX_HEXSZ + 1], type_str[32];
		unsigned long size;
		if (sscanf(line.buf, "%s %31s %lu", oid_hex, type_str, &size) != 3)
			continue;

		struct object_id oid;
		if (get_oid_hex_any(oid_hex, &oid) == GIT_HASH_UNKNOWN)
			continue;

		struct object_info oi = OBJECT_INFO_INIT;
		enum object_type type = type_from_string(type_str);
		if (request && request->typep) oi.typep = &type;
		if (request && request->sizep) oi.sizep = &size;

		int ret = cb(&oid, &oi, cb_data);
		if (ret) { strbuf_release(&line); return ret; }
	}
	strbuf_release(&line);
	return 0;
}

static int helper_source_freshen_object(struct odb_source *source,
					const struct object_id *oid)
{
	struct odb_source_helper *src = (struct odb_source_helper *)source;
	char hex[GIT_MAX_HEXSZ + 1];
	oid_to_hex_r(hex, oid);

	helper_send(src, "have %s\n", hex);

	struct strbuf line = STRBUF_INIT;
	if (helper_readline(src, &line) == EOF) {
		strbuf_release(&line);
		return 0;
	}
	int exists = !strcmp(line.buf, "true");
	strbuf_release(&line);
	return exists;
}

static int helper_source_write_object(struct odb_source *source,
				      const void *buf, unsigned long len,
				      enum object_type type,
				      struct object_id *oid,
				      struct object_id *compat_oid,
				      unsigned flags UNUSED)
{
	struct odb_source_helper *src = (struct odb_source_helper *)source;

	/* Compute OID locally */
	hash_object_file(src->base.odb->repo->hash_algo, buf, len, type, oid);

	/* Send OID with the put command so helper can store it directly */
	char hex[GIT_MAX_HEXSZ + 1];
	oid_to_hex_r(hex, oid);
	helper_send(src, "put %s %s %lu\n", hex, type_name(type), len);
	write_in_full(get_helper(src)->in, buf, len);

	struct strbuf line = STRBUF_INIT;
	if (helper_readline(src, &line) == EOF) {
		strbuf_release(&line);
		return -1;
	}

	/* Response is the oid hex (we already computed it, just verify) */
	strbuf_release(&line);

	if (compat_oid)
		oidcpy(compat_oid, oid);
	return 0;
}

static int helper_source_write_object_stream(struct odb_source *source UNUSED,
					     struct odb_write_stream *stream UNUSED,
					     size_t len UNUSED,
					     struct object_id *oid UNUSED)
{
	return -1;
}

static int helper_source_begin_transaction(struct odb_source *source UNUSED,
					   struct odb_transaction **out UNUSED)
{
	return -1;
}

static int helper_source_read_alternates(struct odb_source *source UNUSED,
					 struct strvec *out UNUSED)
{
	return 0;
}

static int helper_source_write_alternate(struct odb_source *source UNUSED,
					 const char *alternate UNUSED)
{
	return -1;
}

/* ---- Constructor ---- */

struct odb_source_helper *odb_source_helper_new(struct object_database *odb,
						const char *helper_name,
						const char *path,
						bool local)
{
	struct odb_source_helper *src;
	CALLOC_ARRAY(src, 1);
	odb_source_init(&src->base, odb, ODB_SOURCE_HELPER, path, local);
	src->helper_name = xstrdup(helper_name);
	src->base.free = helper_source_free;
	src->base.close = helper_source_close;
	src->base.reprepare = helper_source_reprepare;
	src->base.read_object_info = helper_source_read_object_info;
	src->base.read_object_stream = helper_source_read_object_stream;
	src->base.for_each_object = helper_source_for_each_object;
	src->base.freshen_object = helper_source_freshen_object;
	src->base.write_object = helper_source_write_object;
	src->base.write_object_stream = helper_source_write_object_stream;
	src->base.begin_transaction = helper_source_begin_transaction;
	src->base.read_alternates = helper_source_read_alternates;
	src->base.write_alternate = helper_source_write_alternate;
	return src;
}
