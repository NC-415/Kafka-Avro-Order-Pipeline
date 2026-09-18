import tarfile, os, shutil

print("Opening tarball...")
with tarfile.open('D:\\confluent-7.7.0.tar.gz') as t:
    members = []
    for m in t.getmembers():
        if m.name.startswith('confluent-7.7.0/share/java/'):
            if 'confluent-control-center' in m.name or 'ksqldb' in m.name or 'kafka-connect' in m.name or 'confluent-metadata' in m.name:
                continue
            members.append(m)
    print(f"Extracting {len(members)} java dependencies...")
    t.extractall('D:\\', members=members)

print("Moving extracted files to D:\\confluent\\share\\java...")
source_dir = 'D:\\confluent-7.7.0\\share\\java'
dest_dir = 'D:\\confluent\\share\\java'
os.makedirs(dest_dir, exist_ok=True)
for item in os.listdir(source_dir):
    s = os.path.join(source_dir, item)
    d = os.path.join(dest_dir, item)
    if not os.path.exists(d):
        shutil.move(s, d)
print("Done.")
