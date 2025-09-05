# scripts/run_backfill.py
import subprocess
import sys
import argparse
from pathlib import Path

# 이 스크립트가 있는 'scripts' 폴더의 경로를 기준으로 경로 설정
SCRIPTS_DIR = Path(__file__).resolve().parent

def run_command(command: list[str]):
    """주어진 명령어를 실행하고 오류가 발생하면 프로그램을 중단합니다."""
    print("\n" + "="*50)
    print(f"🚀 실행 중: {' '.join(command)}")
    print("="*50)
    try:
        # check=True는 명령어가 0이 아닌 종료 코드를 반환할 경우 예외를 발생시킵니다.
        subprocess.run(command, check=True, text=True)
        print(f"✅ 완료: {' '.join(command)}")
    except FileNotFoundError:
        print(f"❌ 오류: '{command[0]}'를 찾을 수 없습니다. Python이 PATH에 설정되었는지 확인하세요.")
        sys.exit(1)
    except subprocess.CalledProcessError as e:
        print(f"❌ 오류: 다음 명령어를 실행하는 중 문제가 발생했습니다: {' '.join(command)}")
        print(f"종료 코드: {e.returncode}")
        sys.exit(1)
    except KeyboardInterrupt:
        print("\n사용자에 의해 작업이 중단되었습니다.")
        sys.exit(1)

def main():
    parser = argparse.ArgumentParser(
        description="""과거 연도의 영화 데이터를 순차적으로 가져와 최종 검색 파일까지 생성하는 마스터 스크립트.
                       API 사용량 제한을 피하기 위해 연도별로 작업을 나누어 실행하는 것을 권장합니다."""
    )
    parser.add_argument("--year-start", type=int, required=True, help="데이터 수집 시작 연도")
    parser.add_argument("--year-end", type=int, required=True, help="데이터 수집 종료 연도")
    args = parser.parse_args()

    y_start, y_end = args.year_start, args.year_end

    print(f"🗓️ {y_start}년부터 {y_end}년까지의 영화 데이터 보강 작업을 시작합니다.")
    
    # --- 1단계: 연도별 영화 '목록' 파일 생성 ---
    # build_year_cache.py를 사용하여 `year-YYYY.json` 파일들을 생성합니다.
    print("\n[1/3] 연도별 영화 목록 파일을 생성합니다...")
    cmd_step1 = [
        "python",
        str(SCRIPTS_DIR / "build_year_cache.py"),
        "--year-start", str(y_start),
        "--year-end", str(y_end)
    ]
    run_command(cmd_step1)

    # --- 2단계: 영화 '상세 정보' 파일 가져오기 ---
    # build_movie_details.py를 사용하여 각 영화의 상세 정보를 가져옵니다.
    # API 제한을 피하기 위해 한 해씩 순차적으로 실행합니다.
    print("\n[2/3] 영화 상세 정보 파일을 가져옵니다. (API 사용으로 시간이 걸릴 수 있습니다)")
    for year in range(y_start, y_end + 1):
        print(f"\n--- {year}년 상세 정보 처리 시작 ---")
        cmd_step2_year = [
            "python",
            str(SCRIPTS_DIR / "build_movie_details.py"),
            "--year-start", str(year),
            "--year-end", str(year)
        ]
        run_command(cmd_step2_year)
    
    # --- 3단계: 통합 검색 파일 다시 만들기 ---
    # build_indices.py를 사용하여 웹사이트가 사용할 최종 데이터 파일을 생성합니다.
    print("\n[3/3] 최종 검색 인덱스 파일을 생성합니다...")
    cmd_step3 = ["python", str(SCRIPTS_DIR / "build_indices.py")]
    run_command(cmd_step3)

    print("\n🎉 모든 데이터 보강 작업이 성공적으로 완료되었습니다!")

if __name__ == "__main__":
    main()
