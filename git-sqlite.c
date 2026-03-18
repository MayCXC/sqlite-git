/*
 * git-sqlite: unified binary for local and remote SQLite helpers.
 *
 * Dispatches on argv[0]:
 *   git-local-sqlite  -> local helper protocol (objects + refs + reflogs)
 *   git-remote-sqlite -> remote helper protocol (list + fetch + push)
 */
#include <string.h>
#include <stdio.h>

int local_main(int argc, char **argv);
int remote_main(int argc, char **argv);

int main(int argc, char **argv) {
	const char *name = strrchr(argv[0], '/');
	name = name ? name + 1 : argv[0];

	if (!strcmp(name, "git-local-sqlite"))
		return local_main(argc, argv);
	if (!strcmp(name, "git-remote-sqlite"))
		return remote_main(argc, argv);

	fprintf(stderr, "git-sqlite: invoke as git-local-sqlite or git-remote-sqlite\n");
	return 1;
}
