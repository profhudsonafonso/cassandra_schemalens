#!/usr/bin/env bash
# Gera os dados cpu-only com o gerador OFICIAL do TSBS (Go), commit fixado, num container golang descartavel.
# Saida: data/tsbs_cpu_only_cassandra.txt.gz (+ data/GENERATION.json com versoes e SHA-256).
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMMIT=8323e59c74027b108f4ad5ec5d3e498b0101a02e
GO_IMAGE=golang:1.22.5
OUT="$HERE/data"
mkdir -p "$OUT"
[[ -f "$OUT/tsbs_cpu_only_cassandra.txt.gz" ]] && { echo "dados ja existem (nao sobrescreve)"; exit 0; }
docker run --rm --cpuset-cpus 16-23 -v "$OUT":/out "$GO_IMAGE" bash -c "
  set -e
  go install github.com/timescale/tsbs/cmd/tsbs_generate_data@$COMMIT
  tsbs_generate_data --use-case=cpu-only --seed=20261117 --scale=40 \
     --timestamp-start=2020-01-01T00:00:00Z --timestamp-end=2020-01-02T12:00:00Z \
     --log-interval=600s --format=cassandra | gzip -n > /out/tsbs_cpu_only_cassandra.txt.gz
  go version > /out/go_version.txt
  go version -m \$(go env GOPATH)/bin/tsbs_generate_data > /out/tsbs_build_info.txt
  chown $(id -u):$(id -g) /out/*
"
python3 - "$OUT" "$GO_IMAGE" "$COMMIT" <<'EOF'
import hashlib, json, sys, subprocess, pathlib
out, img, commit = pathlib.Path(sys.argv[1]), sys.argv[2], sys.argv[3]
f = out / 'tsbs_cpu_only_cassandra.txt.gz'
digest = subprocess.run(['docker', 'image', 'inspect', img, '--format', '{{json .RepoDigests}}'], capture_output=True, text=True).stdout.strip()
json.dump({'tsbs_commit': commit, 'go_image': img, 'go_image_digest': digest,
           'command': 'tsbs_generate_data --use-case=cpu-only --seed=20261117 --scale=40 --timestamp-start=2020-01-01T00:00:00Z --timestamp-end=2020-01-02T12:00:00Z --log-interval=600s --format=cassandra',
           'sha256': hashlib.sha256(f.read_bytes()).hexdigest(), 'bytes': f.stat().st_size,
           'go_version': (out / 'go_version.txt').read_text().strip()}, open(out / 'GENERATION.json', 'w'), indent=2)
print(open(out / 'GENERATION.json').read())
EOF
