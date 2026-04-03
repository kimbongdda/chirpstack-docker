# 패키지 워크플로

이 저장소는 두 가지 배포/이전 흐름을 지원합니다.

## 1. 공개용 Export

민감정보 없이 프로젝트만 공유하거나 공개 저장소에 올릴 때 사용합니다.

```powershell
.\scripts\export-public.ps1
```

생성 위치:

* `artifacts/public/chirpstack-docker-public-YYYYMMDD-HHMMSS.zip`

포함:

* 소스 코드
* Docker Compose 파일
* 설정 파일
* `dashboard/.env.example`

제외:

* `dashboard/.env`
* PostgreSQL dump
* SQLite DB 파일
* Redis dump 파일
* 기존 백업 산출물

## 2. 내 전용 Migration Package

현재 노트북의 환경을 다른 PC나 노트북으로 그대로 옮길 때 사용합니다.

```powershell
.\scripts\backup-private.ps1
```

생성 위치:

* `artifacts/private/chirpstack-docker-private-YYYYMMDD-HHMMSS.zip`

이 패키지에는 아래 내용이 들어갑니다.

* 안전한 프로젝트 스냅샷
* `migration-data/dashboard.env`
* `migration-data/chirpstack.sql`
* `migration-data/lora_history.db`
* `migration-data/dump.rdb`

이 패키지는 개인용입니다. GitHub 업로드나 공개 공유는 하면 안 됩니다.

## 다른 노트북에서 복원하기

1. private migration zip 을 압축 해제합니다.
2. 압축 해제한 프로젝트 폴더에서 PowerShell 을 엽니다.
3. 아래 명령을 실행합니다.

```powershell
.\scripts\restore-private.ps1
```

기본값으로 Docker named volume 을 초기화한 뒤 백업 데이터를 복원하므로,
원본 환경과 최대한 같은 상태로 맞출 수 있습니다.

## 추천 사용법

* GitHub 공유용: `.\scripts\export-public.ps1`
* 내 장비 이전용: `.\scripts\backup-private.ps1`
* 새 노트북 복원용: `.\scripts\restore-private.ps1`
