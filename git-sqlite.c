/*
 * git-sqlite: unified binary for local and remote SQLite helpers.
 *
 * Dispatches on argv[0]:
 *   git-local-sqlite  -> local helper protocol (objects + refs + reflogs)
 *   git-remote-sqlite -> remote helper protocol (list + fetch + push)
 *
 * Install as git-local-sqlite and symlink git-remote-sqlite to it,
 * or vice versa.
 */
#include <string.h>
#include <stdio.h>

int local_main(int argc, char **argv);
int remote_main(int argc, char **argv);

int main(int argc, char **argv) {
	const char *name = strrchr(argv[0], '/');
	name = name ? name + 1 : argv[0];

	if (!strcmp(name, "git-remote-sqlite"))
		return remote_main(argc, argv);

	return local_main(argc, argv);
}
