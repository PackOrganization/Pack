<h1 align="center">Pack</h1>

Container for files and raw data. Safe, fast, and reliable.

Get the latest version from https://pack.ac



## Running

For packing some files, simply write:

```
pack ./test/
```

And for unpacking:

```
pack ./test.pack
```

Use `--help` parameter for more options.

For example, here is how to overwrite the output:

```
pack -i ./test/ -o ./test.pack -w
```



## Some numbers:

Packing a copy of Linux source code containing more than 81K files and around 1.3GB on Windows (with NTFS):

- tar: 4.7s, 1.31 GB
- tar.gz: 28.5s, 214 MB
- RAR: 27.5s, 235 MB
- Zip: 146s, 253 MB
- 7z: 54.2s, 135 MB
- Pack: 1.3s, 194 MB



On Linux (with ext4) it is even faster:

- tar.gz: 27.5 s
- Pack: 0.96 s



> [!NOTE]
> Numbers are from corresponding official programs in an out-of-the-box configuration. And all are considered to be in a warm state with no antivirus interference.
> On the first try of reading many files, Windows Defender (or any other antivirus) makes everything slow as it is scanning the files.
> Please test for yourself and run any test at least two times.


## Notes:

- It is Free and remains as is. Made to enable people to have a safer, easier and faster life.
- Source code is available with a permissive licence.
- It is at the beta stage; keep the input files. It is designed to be safe, have crash resistance, and prevent reading problematic data and exploited by many vulnerabilities, but for now it is intended only for evaluation purposes.
- It is fast—really fast.
- It is smart. It configures itself as needed; there are not many dials to play with.
- It is very resource-friendly.
- The next updates will include exploring, encryption, Library and more.



## Build:
Here are some steps to follow:
- You will need:
  - FreePascal and Lazarus from https://gitlab.com/freepascal.org
  - SCL from https://github.com/SCLOrganization
- Using Lazarus:
  - Open SCL package file (SCL/Package/SCL.lpk)
    - The standard library that used to make Pack
    - You will need SCL Source and SCL Libraries in the same directory
  - Open Pack package file (Pack/Package/PackPackage.lpk)
     - Base package for Pack source code
  - Open Pack Draft0 package file (Pack/Package/Draft0/PackDraft0Package.lpk)
     - It adds support for version Draft 0 of Pack Format
  - Open Pack CLI project (Pack/CLI/CLIProject.lpi)
  - Build

---
A gift to anyone passionate about data especially, Phil Katz, D. Richard Hipp, Yann Collet and me.
