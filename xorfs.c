/**
 * XOR Filesystem
 *
 * Assistance for XOR backups
 *
 * Filesystem providing plain backup data files (disk image files)
 * from data stored in plain images and chains of xored images.
 *
 * Author: Tomáš Binek <tomasbinek@seznam.cz>
 * License: GNU GPL
 *
 * Used materials:
 * http://www.maastaar.net/fuse/linux/filesystem/c/2016/05/21/writing-a-simple-filesystem-using-fuse/
 */

#define FUSE_USE_VERSION 30

#include <fuse.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <dirent.h>
#include <time.h>
#include <string.h>
#include <stdlib.h>
#include <stdarg.h>
#include <stdint.h>
#include <errno.h>

#define XORFS_VERSION_MAJOR 0
#define XORFS_VERSION_MINOR 1

#define XORFS_LOG_ERROR 1
#define XORFS_LOG_WARNING 2
#define XORFS_LOG_NOTICE 3
#define XORFS_LOG_INFO 4
#define XORFS_LOG_DEBUG 5

#define XORFS_LOG_LEVEL 5
#define XORFS_DEBUG_FILE_NAME "debug.info"
#define XORFS_SOURCE_FILE_EXTENSION ".xor"
#define XORFS_ROOT_PERMISSIONS 0755
#define XORFS_FILE_PERMISSIONS 0644

const char* XORFS_LOG_LEVEL_NAMES[] = { "_NA", "ERROR", "WARNING", "NOTICE", "INFO", "DEBUG" };

struct xorfs_backup {
   char *name;
   unsigned int number;
   unsigned int xor_against_number;
   struct xorfs_source_file* xor_against_source_file;
   time_t time;
   char *output_file_name;
};

struct xorfs_source_file {
    char *name; // Allocated string
    FILE *file_descriptor;
    struct stat stat;
    struct xorfs_backup backup;
};

struct xorfs_source_files {
    unsigned int count;
    struct xorfs_source_file* files;
};

/* Maybe convert these to a structure? */
char *xorfs_source_directory_path = NULL;
struct xorfs_source_files xorfs_source_files = { 0, NULL };
int xorfs_debug_file_fd = -1;


int xorfs_log(int severity, const char *format, ...)
{
    int return_code;
    va_list args;
    va_start(args, format);

    if (severity <= XORFS_LOG_LEVEL)
    {
        fprintf(stderr, "[%s] ", XORFS_LOG_LEVEL_NAMES[severity]);
        return_code = vfprintf(stderr, format, args);
    }

    va_end(args);
    return return_code;
}

struct xorfs_source_file* xorfs_get_source_file_by_file_name(const char *requested_name)
{
   for (int index = 0; index < xorfs_source_files.count; index++)
   {
      if (strcmp(xorfs_source_files.files[index].backup.output_file_name, requested_name) == 0)
      {
         return xorfs_source_files.files + index;
      }
   }

   xorfs_log(XORFS_LOG_NOTICE, "Source file for output file '%s' not found\n", requested_name);
   return NULL;
}

static int xorfs_operation_getattr( const char *path, struct stat *st )
{
   xorfs_log(XORFS_LOG_DEBUG, "Operation 'getattr' on '%s'\n", path);

   // Root directory
   if (strcmp(path, "/") == 0)
   {
      st->st_mode = S_IFDIR | XORFS_ROOT_PERMISSIONS;
      st->st_nlink = 2; // Why "two" hardlinks instead of "one"? The answer is here: http://unix.stackexchange.com/a/101536
   }
   // Debug file
   else if (strcmp(path + 1, XORFS_DEBUG_FILE_NAME) == 0)
   {
      st->st_atime = st->st_mtime = st->st_ctime = time(NULL);
      st->st_nlink = 1;
      st->st_mode = S_IFREG | XORFS_FILE_PERMISSIONS;
      st->st_size = lseek(xorfs_debug_file_fd, 0, SEEK_END);
   }
   // A source file
   else
   {
      struct xorfs_source_file *source_file = xorfs_get_source_file_by_file_name(path + 1 /* removing slash */);

      if (source_file != NULL)
      {
         // Copy from source file
         *st = source_file->stat;

         // Overwrite some
         st->st_atime = time(NULL);
         st->st_mtime = source_file->backup.time;
         st->st_ctime = source_file->backup.time;
         st->st_nlink = 1;
         st->st_mode = S_IFREG | XORFS_FILE_PERMISSIONS;
      }
      else
      {
         return -ENOENT;
      }
   }

   // Common attributes
   st->st_uid = getuid(); // The owner of the file/directory is the user who mounted the filesystem
   st->st_gid = getgid(); // The group of the file/directory is the same as the group of the user who mounted the filesystem

   return 0;
}

static int xorfs_operation_readdir( const char *path, void *buffer, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi )
{
        printf( "--> Getting The List of Files of %s\n", path );

        filler( buffer, ".", NULL, 0 ); // Current Directory
        filler( buffer, "..", NULL, 0 ); // Parent Directory

        // Root directory
        if ( strcmp( path, "/" ) == 0 )
        {
           // Source files
           for (int index = 0; index < xorfs_source_files.count; index++)
           {
              filler(buffer, xorfs_source_files.files[index].backup.output_file_name, NULL, 0);
           }

           // Debug file
           filler(buffer, XORFS_DEBUG_FILE_NAME, NULL, 0);

           return 0;
        }

        return -ENOENT;
}

int xorfs_read_plain(struct xorfs_source_file *source_file, char *buffer, off_t offset, size_t size)
{
   // Set position
   int fseek_result = fseek(source_file->file_descriptor, offset, SEEK_SET);
   if (fseek_result < 0)
   {
      xorfs_log(XORFS_LOG_ERROR, "Cannot fseek file %s to offset %li: %s\n", source_file->name, offset, strerror(errno));
      return -EINVAL;
   }

   // Read data
   int read_bytes = fread(buffer, 1, size, source_file->file_descriptor);

   return read_bytes;
}

int xorfs_read_backup(struct xorfs_source_file *source_file, char *buffer, off_t offset, size_t size)
{
   xorfs_log(XORFS_LOG_DEBUG, "Read backup %s-%i, offset %li\n", source_file->backup.name, source_file->backup.number, offset);

   int read_bytes = 0;

   // Read from the requested file
   {
      int r = xorfs_read_plain(source_file, buffer, offset, size);
      if (r < 0)
      {
         return r;
      }

      read_bytes = r;
   }

   if (source_file->backup.xor_against_number == 0)
   // Reading plain image
   {
      // Data is already in the buffer,
      // just return the number of bytes
      return read_bytes;
   }
   else
   // Reading xored image
   {
      char *first_buffer = buffer;
      char *second_buffer = NULL;

      // Allocate second buffer
      {
         second_buffer = malloc(size);
         if (second_buffer == NULL)
         {
            return -ENOMEM;
         }
      }

      // Read from the second file
      {
         int r = xorfs_read_backup(source_file->backup.xor_against_source_file, second_buffer, offset, read_bytes);
         if (r < 0)
         {
            free(second_buffer);
            return r;
         }
         else if (r != read_bytes)
         {
            xorfs_log(XORFS_LOG_ERROR, "Read mismatch: %i bytes were read from %s, but only %i from %s\n", read_bytes, source_file->name, r, source_file->backup.xor_against_source_file->name);
            free(second_buffer);
            return -EIO;
         }
      }

      // Xor buffers
      {
         int uintmax_size = sizeof (uintmax_t);
         if ((size % uintmax_size) == 0)
         // Use uintmax_t chunks
         {
            xorfs_log(XORFS_LOG_DEBUG, "Using uintmax_t chunks of size %i for the xoring\n", uintmax_size);

            uintmax_t* first_buffer_maxint = (uintmax_t *) first_buffer;
            uintmax_t* second_buffer_maxint = (uintmax_t *) second_buffer;

            int size_in_uintmax_chunks = size / uintmax_size;
            int i = 0;
            while (i < size_in_uintmax_chunks)
            {
               first_buffer_maxint[i] ^= second_buffer_maxint[i];
               i++;
            }
         }
         else
         // Go byte-by-byte
         {
            xorfs_log(XORFS_LOG_DEBUG, "Using byte-by-byte operation for the xoring\n", uintmax_size);

            int i = 0;
            while (i < size)
            {
               first_buffer[i] ^= second_buffer[i];
               i++;
            }
         }
      }

      free(second_buffer);
      return read_bytes;
   }
}

static int xorfs_operation_read( const char *path, char *buffer, size_t size, off_t offset, struct fuse_file_info *fi )
{
   xorfs_log(XORFS_LOG_DEBUG, "Operation read on '%s', offset %li, size %li\n", path, offset, size);

   // Reading debug file
   if (strcmp(path + 1, XORFS_DEBUG_FILE_NAME) == 0)
   {
     lseek(xorfs_debug_file_fd, offset, SEEK_SET);
     ssize_t read_result = read(xorfs_debug_file_fd, buffer, size);
     return read_result;
   }
   else
   // Reading source file
   {
      struct xorfs_source_file *source_file = xorfs_get_source_file_by_file_name(path + 1 /* removing slash */);
      if (source_file == NULL)
      {
         return -ENOENT;
      }

      return xorfs_read_backup(source_file, buffer, offset, size);
   }
}

static struct fuse_operations operations = {
    .getattr	= xorfs_operation_getattr,
    .readdir	= xorfs_operation_readdir,
    .read		= xorfs_operation_read,
};

static int xorfs_process_argument(void *data, const char *arg, int key, struct fuse_args *outargs)
{
    xorfs_log(XORFS_LOG_DEBUG, "Processing argument \"%s\", key %i\n", arg, key);

    // Take the first non-option as source directory
    if (key == FUSE_OPT_KEY_NONOPT && xorfs_source_directory_path == NULL)
    {
        xorfs_source_directory_path = strdup(arg);

        // Do not pass this argument to fuse itself
        return 0;
    }

    return 1;
}

void xorfs_close_source_files ()
{
   // Close all files
   for (int index = 0; index < xorfs_source_files.count; index++)
   {
       char* file_name = xorfs_source_files.files[index].name;
       char *backup_name = xorfs_source_files.files[index].backup.name;
       char *backup_output_file_name = xorfs_source_files.files[index].backup.output_file_name;
       FILE* file_descriptor = xorfs_source_files.files[index].file_descriptor;

       xorfs_log(XORFS_LOG_DEBUG, "Closing file '%s'\n", file_name);

       free(file_name);
       free(backup_name);
       free(backup_output_file_name);
       if (file_descriptor != NULL) { fclose(file_descriptor); }
   }

   // Free memory
   free(xorfs_source_files.files);
}

struct xorfs_source_file* get_source_file_by_backup_name_and_number(const char* requested_name, unsigned int requested_number)
{
   for (int index = 0; index < xorfs_source_files.count; index++)
   {
      if (xorfs_source_files.files[index].backup.number == requested_number && strcmp(xorfs_source_files.files[index].backup.name, requested_name) == 0)
      {
         return xorfs_source_files.files + index;
      }
   }

   xorfs_log(XORFS_LOG_NOTICE, "Source file for backup %s-%i not found\n", requested_name, requested_number);
   return NULL;
}

int xorfs_create_debug_file(struct xorfs_source_files *source_files)
{
   char mkstemp_template[] = "/tmp/xorfs-debug-XXXXXX";
   int fd = mkstemp(mkstemp_template);
   if (fd < 0)
   {
      xorfs_log(XORFS_LOG_ERROR, "Unable to create temporary file\n");
      return -1;
   }

   dprintf(fd, "XORFS\n");
   dprintf(fd, "version: %i.%i\n", XORFS_VERSION_MAJOR, XORFS_VERSION_MINOR);
   dprintf(fd, "----------------------------------------\n\n");

   dprintf(fd, "Source files:\n");
   dprintf(fd, "total %i\n\n", xorfs_source_files.count);

   for (int file_index = 0; file_index < xorfs_source_files.count; file_index++)
   {
      struct xorfs_source_file *sf = xorfs_source_files.files + file_index;

      dprintf(fd, "Source file #%i, at %p\n", file_index, sf);
      dprintf(fd, " - File name: %s\n", sf->name);
      dprintf(fd, " - Backup:\n");
      dprintf(fd, "   - Name: %s\n", sf->backup.name);
      dprintf(fd, "   - Number: %i\n", sf->backup.number);
      dprintf(fd, "   - Xored against number (link): %i (%p)\n", sf->backup.xor_against_number, sf->backup.xor_against_source_file);
   }

   return fd;
}

int xorfs_open_source_files (char *directory_path)
{
    int return_value;
    DIR* source_directory;

    // Open the directory
    source_directory = opendir(directory_path);
    if (source_directory == NULL)
    {
        xorfs_log(XORFS_LOG_ERROR, "Unable to open '%s' as source directory\n", directory_path);
        return_value = 1;
        goto failure_return;
    }

    // Process entries one-by-one
    {
       struct dirent* entry;

       while (entry = readdir(source_directory))
       // Process a directory entry
       {
           xorfs_log(XORFS_LOG_DEBUG, "Source file: '%s', inode %i, type %i\n", entry->d_name, entry->d_ino, entry->d_type);

           // Filter out unwanted entries
           {
               // Process only regular files
               if (entry->d_type != DT_REG && entry->d_type != DT_UNKNOWN)
               {
                   xorfs_log(XORFS_LOG_DEBUG, "Ignoring file '%s' - not a regular file\n", entry->d_name);
                   continue;
               }

               // Process only .xor files
               char *last_four_characters = entry->d_name + (strlen(entry->d_name) - strlen(XORFS_SOURCE_FILE_EXTENSION));
               if (last_four_characters <= entry->d_name || (strcmp(last_four_characters, XORFS_SOURCE_FILE_EXTENSION) != 0))
               {
                   xorfs_log(XORFS_LOG_DEBUG, "Ignoring file '%s' - name not ending with '%s'\n", entry->d_name, XORFS_SOURCE_FILE_EXTENSION);
                   continue;
               }
           }

           // Open a .xor file, store information about the file
           {
               // Allocate more memory for `struct xorfs_source_files`
               {
                   int new_count = xorfs_source_files.count + 1;
                   size_t new_size = new_count * sizeof(struct xorfs_source_file);
                   struct xorfs_source_file* new_memory = realloc(xorfs_source_files.files, new_size);
                   if (new_memory == NULL)
                   {
                       xorfs_log(XORFS_LOG_ERROR, "Unable to allocate %l bytes of memory: %s\n", new_size, strerror(errno));
                       return_value = 2;
                       goto failure_close_files;
                   }

                   xorfs_source_files.files = new_memory;
                   xorfs_source_files.count = new_count;
               }

               // Fill the new `struct xorfs_source_file`
               {
                   int index = xorfs_source_files.count - 1;
                   struct xorfs_source_file* new_source_file = xorfs_source_files.files + index;
                   char *file_name = entry->d_name;
                   FILE* file_descriptor;

                   // Safe-fill the structure
                   {
                      new_source_file->name = NULL;
                      new_source_file->file_descriptor = NULL;
                      new_source_file->backup.name = NULL;
                      new_source_file->backup.number = 0;
                      new_source_file->backup.xor_against_number = 0;
                      new_source_file->backup.xor_against_source_file = NULL;
                      new_source_file->backup.output_file_name = NULL;
                   }

                   // Obtain the file descriptor
                   {
                       char *file_path = NULL;

                       // Construct path to file
                       {
                           // directory path, slash, file name, null byte
                           size_t path_buffer_size = strlen(xorfs_source_directory_path) + 1 + strlen(file_name) + 1;

                           file_path = malloc(path_buffer_size);
                           if (file_path == NULL)
                           {
                               xorfs_log(XORFS_LOG_ERROR, "Unable to allocate memory: %s\n", strerror(errno));
                               return_value = 3;
                               goto failure_close_files;
                           }

                           file_path[0] = '\0';

                           strcat(file_path, xorfs_source_directory_path);
                           strcat(file_path, "/");
                           strcat(file_path, file_name);
                       }

                       // Open the file
                       {
                          file_descriptor = fopen(file_path, "r");
                          if (file_descriptor == NULL)
                          {
                              xorfs_log(XORFS_LOG_ERROR, "Unable to open file '%s': %s\n", file_path, strerror(errno));
                              free(file_path);
                              return_value = 3;
                              goto failure_close_files;
                          }
                          else
                          {
                              xorfs_log(XORFS_LOG_DEBUG, "Successfully opened file '%s'\n", file_path);
                          }
                       }

                       free(file_path);
                       new_source_file->file_descriptor = file_descriptor;
                   }

                   // Copy name string
                   {
                      char* duplicated_name = strdup(file_name);
                      if (duplicated_name == NULL)
                      {
                         xorfs_log(XORFS_LOG_ERROR, "Unable to allocate memory: %s\n", strerror(errno));
                         return_value = 3;
                         goto failure_close_files;
                      }

                      new_source_file->name = duplicated_name;
                   }

                   // Get stat info
                   {
                      int stat_result = fstat(fileno(file_descriptor), &(new_source_file->stat));
                      if (stat_result != 0)
                      {
                         xorfs_log(XORFS_LOG_ERROR, "Unable to stat file '%s': %s\n", file_name, strerror(errno));
                         return_value = 4;
                         goto failure_close_files;
                      }
                   }

                   // Get backup information
                   {
                      struct xorfs_backup* backup_info = &(new_source_file->backup);

                      // Parse file name
                      {
                         int first_number_offset = 0;
                         char* xOrDot;
                         char *dot;

                         // Characters until the first number are name
                         {
                            char current_char;
                            int maximum_offset = strlen(new_source_file->name) - 1;

                            // Find offset of first number
                            do
                            {
                               current_char = new_source_file->name[first_number_offset];

                               if (current_char >= '0' && current_char <= '9')
                               {
                                  xorfs_log(XORFS_LOG_DEBUG, "First number in '%s' found at offset %i\n", new_source_file->name, first_number_offset);
                                  break;
                               }
                               else if (first_number_offset == maximum_offset)
                               {
                                  xorfs_log(XORFS_LOG_ERROR, "Malformed backup file name '%s': Unable to find first number\n", new_source_file->name);
                                  return_value = 5;
                                  goto failure_close_files;
                               }
                               else
                               {
                                  first_number_offset++;
                                  // and continue
                               }
                            }
                            while (1);

                            // Copy name
                            {
                               int name_end_offset = first_number_offset - 1; // Remove trailing dash
                               void* new_memory = malloc(name_end_offset + 1); // name + \0
                               if (new_memory == NULL)
                               {
                                  xorfs_log(XORFS_LOG_ERROR, "Unable to allocate memory: %s\n", strerror(errno));
                                  return_value = 5;
                                  goto failure_close_files;
                               }

                               backup_info->name = new_memory;

                               // Copy the string
                               strncpy(backup_info->name, new_source_file->name, name_end_offset);

                               // Write null byte
                               backup_info->name[name_end_offset] = '\0';
                            }
                         }

                         // Read first number
                         {
                            unsigned long int number = 0;
                            number = strtol(new_source_file->name + first_number_offset, &xOrDot, 10);
                            // No error checking - there is a number at the beginning of the string here, always

                            backup_info->number = (int) number;
                         }

                         // Then the rest of the name
                         {
                            if (*xOrDot == 'x')
                            {
                               // Read second number
                               {
                                  unsigned long int number = 0;
                                  errno = 0;
                                  number = strtol(xOrDot + 1, &dot, 10);
                                  if (errno != 0)
                                  {
                                     xorfs_log(XORFS_LOG_ERROR, "Malformed backup file name '%s': Cannot read second number after 'x'\n", new_source_file->name);
                                     return_value = 5;
                                     goto failure_close_files;
                                  }

                                  backup_info->xor_against_number = (int) number;
                               }
                            }
                            else if (*xOrDot == '.')
                            {
                               // This is a plain image file
                               backup_info->xor_against_number = 0;
                               backup_info->xor_against_source_file = NULL;
                               dot = xOrDot;
                            }
                            else
                            {
                               xorfs_log(XORFS_LOG_ERROR, "Malformed backup file name '%s': No 'x' or '.' after the first number\n", new_source_file->name);
                               return_value = 5;
                               goto failure_close_files;
                            }
                         }

                         // Verify that we ended on a '.'
                         if (*dot != '.')
                         {
                            xorfs_log(XORFS_LOG_ERROR, "Malformed backup file name '%s': Dot not found when expected\n", new_source_file->name);
                            return_value = 5;
                            goto failure_close_files;
                         }

                         // Construct output file name
                         {
                            char *output_file_name = strdup(file_name);
                            if (output_file_name == NULL)
                            {
                               xorfs_log(XORFS_LOG_ERROR, "Unable to allocate memory: %s\n", strerror(errno));
                               return_value = 5;
                               goto failure_close_files;
                            }

                            // Overwrite the string after first number with ".dat"
                            int offset_of_dot_or_x = (xOrDot - new_source_file->name);
                            strcpy(output_file_name + offset_of_dot_or_x, ".dat");

                            backup_info->output_file_name = output_file_name;
                         }
                      }

                      // Copy from stat structure
                      backup_info->time = new_source_file->stat.st_mtime;
                   }
               }
           }
       }
    }

    // Check backup links and fill the pointers
    {
       struct xorfs_source_file* source_file;

       for (int index = 0; index < xorfs_source_files.count; index++)
       {
          source_file = xorfs_source_files.files + index;

          if (source_file->backup.xor_against_number == 0)
          // Is plain image file
          {
             // Do nothing
             source_file->backup.xor_against_source_file = NULL;
          }
          else
          // Is xored image file
          {
             struct xorfs_source_file* xor_against_source_file = get_source_file_by_backup_name_and_number(source_file->backup.name, source_file->backup.xor_against_number);

             if (xor_against_source_file != NULL)
             {
                source_file->backup.xor_against_source_file = xor_against_source_file;
             }
             else
             {
                xorfs_log(XORFS_LOG_ERROR, "Backup %s-%i is xored against backup %i, but that is missing\n", source_file->backup.name, source_file->backup.number, source_file->backup.xor_against_number);
                return_value = 6;
                goto failure_close_files;
             }
          }
       }
    }

    // Success
    return 0;

    // Fail procedure
    failure_close_files:
    xorfs_close_source_files();
    failure_closedir:
    closedir(source_directory);
    failure_return:
    return return_value;
}


int main( int argc, char *argv[] )
{
    int fuse_main_return_code;
    struct fuse_args fuse_arguments = FUSE_ARGS_INIT(argc, argv);

    xorfs_log(XORFS_LOG_DEBUG, "Starting\n");

    // Process arguments
    fuse_opt_parse(&fuse_arguments, NULL, NULL, xorfs_process_argument);

    // Open source files
    if (xorfs_open_source_files(xorfs_source_directory_path) != 0)
    {
       xorfs_log(XORFS_LOG_ERROR, "Unable to open source directory and/or files\n");
       return 1;
    }

    // Prepare debug file
    xorfs_debug_file_fd = xorfs_create_debug_file(&xorfs_source_files);

    // Execute fuse main function
    fuse_main_return_code = fuse_main(fuse_arguments.argc, fuse_arguments.argv, &operations, NULL);

    // Cleanup
    {
        xorfs_close_source_files();
    }

    xorfs_log(XORFS_LOG_INFO, "Ending with code %i\n", fuse_main_return_code);
    return fuse_main_return_code;
}
