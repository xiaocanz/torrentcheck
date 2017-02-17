#! /usr/bin/env python
import argparse
import hashlib
import os
import sys

import bencodepy

parser = argparse.ArgumentParser(
    description='Check the integrity of torrent downloads.')
parser.add_argument('directory', help='download directory')
parser.add_argument('torrent', nargs='*', help='torrent file')
parser.add_argument(
    '--delete',
    action='store_true',
    help='delete files that are not found in the torrent file; '
    'do nothing if the torrent file has only a single file')
parser.add_argument(
    '--list-delete',
    action='store_true',
    help='list the files that would be deleted if --delete was set')
parser.add_argument(
    '--debug',
    action='store_true',
    help='print traceback and exit when an exception occurs')

gStrEnc = ['utf8', 'gb2312', 'latin']

def _decode(data):
    return bencodepy.decoder.decode(data)

def main():
    try:
        args = parser.parse_args()
        if not os.path.isdir(args.directory):
            print('{} is not a directory'.format(args.directory))
            return 2
        if args.delete or args.list_delete:
            cmd = delete_cmd
        else:
            cmd = verify_cmd
        all_ok = True
        for torrent_path in args.torrent:
            with open(torrent_path, 'rb') as f:
                torrent = _decode(f.read())
                info = torrent[b'info']
                try:
                    ok = cmd(info, torrent_path, args)
                except Exception:
                    ok = False
                    print('{}: ERROR'.format(torrent_path))
                    if args.debug:
                        raise
                all_ok = all_ok and ok
        return 0 if all_ok else 1
    except KeyboardInterrupt:
        return 1


def delete_cmd(info, torrent_path, args):
    pass
    """ Not tested
    if 'files' not in info:
        return True
    base_path = os.path.join(args.directory, info[b'name'])
    paths = set(os.path.join(base_path, *f[b'path']) for f in info[b'files'])
    count = 0
    for dirpath, dirnames, filenames in os.walk(base_path):
        for filename in filenames:
            p = os.path.join(dirpath, filename)
            if p not in paths:
                count += 1
                if args.list_delete:
                    print('{}: {}'.format(torrent_path, p))
                if args.delete:
                    os.unlink(p)
    if count == 0:
        print('{}: OK'.format(torrent_path))
    else:
        verb = 'deleted' if args.delete else 'found'
        print('{}: {} extra file(s) {}'.format(torrent_path, count, verb))
    return True
    """


def verify_cmd(info, torrent_path, args):
    ok = verify(info, args.directory)
    if ok:
        print('{}: OK'.format(torrent_path))
    else:
        print('{}: FAILED'.format(torrent_path))
    return ok


def _get_base_path(dirpath, bname):
    for enc in gStrEnc:
        try:
            name = bname.decode(enc)
            path = os.path.join(dirpath, name)
            if os.path.isdir(path):
                return path
        except:
            pass
    
    return dirpath


def _get_base_file(dirpath, bname):
    for enc in gStrEnc:
        try:
            name = bname.decode(enc)
            path = os.path.join(dirpath, name)
            if os.path.isfile(path):
                return path
        except:
            pass
    
    return dirpath


def _get_file_path(dirpath, bnames):
    for enc in gStrEnc:
        try:
            filepath = os.path.join(dirpath, *[x.decode(enc) for x in bnames])
            if os.path.isfile(filepath):
                return filepath
        except:
            pass
    
    raise Exception('error')


def verify(info, directory_path, progressor=None):
    """Return True if the checksum values in the torrent file match the
    computed checksum values of downloaded file(s) in the directory and if
    each file has the correct length as specified in the torrent file.
    """
    if b'length' in info:
        base_path = _get_base_file(directory_path, info[b'name'])
        if os.stat(base_path).st_size != info[b'length']:
            return False
        getfile = lambda: open(base_path, 'rb')
    else:
        base_path = _get_base_path(directory_path, info[b'name'])
        assert b'files' in info, 'invalid torrent file'
        for f in info[b'files']:
            p = _get_file_path(base_path, f[b'path'])
            if os.stat(p).st_size != f[b'length']:
                return False
        getfile = lambda: ConcatenatedFile(base_path, info[b'files'])
    with getfile() as f:
        return compare_checksum(info, f, progressor)


class TextProgressor:
    def __init__(self):
        self.mark = 40
    
    def set_total(self, total):
        self.total = total
        if self.total < self.mark:
            self.mark = self.total
        self.current = 0
        self.final = self.mark * self.total
        self.next = self.total
        print('-' * self.mark + '\b' * self.mark, end='', flush=True)

    def tick(self):
        self.current += self.mark
        if self.current >= self.next:
            self.next += self.total
            print('>', end='', flush=True)
        if self.current == self.final:
            print('')


def compare_checksum(info, f, progressor):
    """Return True if the checksum values in the info dictionary match the
    computed checksum values of file content.
    """
    pieces = info[b'pieces']

    def getchunks(f, size):
        while True:
            chunk = f.read(size)
            if chunk == b'':
                break
            yield hashlib.sha1(chunk).digest()

    calc = getchunks(f, info[b'piece length'])
    ref = (pieces[i:i + 20] for i in range(0, len(pieces), 20))
    if progressor: progressor.set_total(len(pieces) // 20)
    for expected, actual in zip(calc, ref):
        if expected != actual:
            return False
        if progressor: progressor.tick()
    return ensure_empty(calc) and ensure_empty(ref)


def ensure_empty(gen):
    """Return True if the generator is empty.  If it is not empty, the first
    element is discarded.
    """
    try:
        next(gen)
        return False
    except StopIteration:
        return True


class ConcatenatedFile(object):
    """A file-like object that acts like a single file whose content is a
    concatenation of the specified files.  The returned object supports read(),
    __enter__() and __exit__().
    """

    def __init__(self, base, files):
        self._base = base
        self._files = files
        self._f = EmptyFile()
        self._i = -1

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._f.close()
        return False

    def read(self, size):
        if self._i == len(self._files):
            return b''
        buf = []
        count = 0
        while True:
            chunk = self._f.read(size - count)
            count += len(chunk)
            buf.append(chunk)
            if count < size:
                self._i += 1
                if self._i == len(self._files):
                    break
                p = _get_file_path(self._base, self._files[self._i][b'path'])
                self._f.close()
                self._f = open(p, 'rb')
            else:
                break
        return b''.join(buf)


class EmptyFile(object):

    def read(self, size):
        return b''

    def close(self):
        return


def check_torrent(torrent_filename, download_dir):
    with open(torrent_filename, "rb") as f:
        torrent = _decode(f.read())
    info = torrent[b'info']
    ok = verify(info, download_dir, TextProgressor())
    return ok


def is_torrent_file(filename):
    if not os.path.isfile(filename): return False
    with open(filename, 'rb') as f:
        b = f.read(1)
    return b == b'd'
    

def do_check(argv):
    """
    Argument : [torrent_filename, download_dir]
    """
    if is_torrent_file(argv[0]):
        ok = check_torrent(argv[0], argv[1])
    elif is_torrent_file(argv[1]):
        ok = check_torrent(argv[1], argv[0])
    else:
        ok = check_torrent(argv[0], argv[1])
    if not ok: raise Exception('Check fail')


if __name__ == '__main__':
    if locals().get('do_' + sys.argv[1]):
        locals()['do_' + sys.argv[1]](sys.argv[2:])
    else:
        exit(main())
