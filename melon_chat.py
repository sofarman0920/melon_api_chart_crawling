import time
import json
import logging
import os
from datetime import datetime, timedelta
from melon import ChartData
from typing import List, Dict
from tqdm import tqdm
import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class MelonChartHistory:
    def __init__(self, imageSize: int = 256, delay_seconds: int = 2):
        self.imageSize = imageSize
        self.delay_seconds = delay_seconds  # delay_seconds20 오류 수정
        self.temp_files = []  # 임시 파일 목록 저장

    def get_chart_by_date(self, target_date: datetime) -> List[Dict]:
        """특정 날짜의 멜론 차트 데이터를 가져옵니다."""
        formatted_date = target_date.strftime('%Y%m%d')
        try:
            chart = ChartData(imageSize=self.imageSize)
            time.sleep(self.delay_seconds)
            chart_data = []
            for entry in chart.entries:
                chart_data.append({
                    '날짜': formatted_date,
                    '시간': target_date.strftime('%H:%M'),
                    '순위': entry.rank,
                    '제목': entry.title,
                    '아티스트': entry.artist,
                    '이전순위': entry.lastPos,
                    '신곡여부': entry.isNew,
                    '앨범이미지': entry.image,
                    '순위변동': entry.rank - entry.lastPos if not entry.isNew else 'NEW'
                })
            return chart_data
        except Exception as e:
            logging.error(f"차트 데이터 수집 실패: {str(e)}")
            return []

    def save_intermediate_data(self, data: List[Dict], current_date: datetime):
        """중간 데이터 저장"""
        filename = f"melon_chart_data_{current_date.strftime('%Y%m%d_%H%M')}.json"
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            self.temp_files.append(filename)  # 임시 파일 목록에 추가
            logging.info(f"중간 데이터 저장 완료: {filename}")
        except Exception as e:
            logging.error(f"데이터 저장 실패: {str(e)}")

    def cleanup_temp_files(self):
        """임시 파일 삭제"""
        for file in self.temp_files:
            try:
                os.remove(file)
                logging.info(f"임시 파일 삭제 완료: {file}")
            except Exception as e:
                logging.error(f"임시 파일 삭제 실패: {str(e)}")
        self.temp_files = []

def get_charts_for_period(start_date: datetime, end_date: datetime,
                         interval: str = 'hour', delay_seconds: int = 2) -> List[Dict]:
    """기간별 차트 데이터를 수집합니다."""
    all_charts = []
    interval_mapping = {
        'year': timedelta(days=365),
        'month': timedelta(days=30),
        'day': timedelta(days=1),
        'hour': timedelta(hours=1)
    }
    interval_delta = interval_mapping.get(interval.lower(), timedelta(hours=1))
    total_iterations = int((end_date - start_date) / interval_delta) + 1
    
    chart_collector = MelonChartHistory(delay_seconds=delay_seconds)
    
    try:
        current_date = start_date
        with tqdm(total=total_iterations, desc="차트 데이터 수집", unit=interval) as pbar:
            while current_date <= end_date:
                daily_chart = chart_collector.get_chart_by_date(current_date)
                all_charts.extend(daily_chart)
                if len(all_charts) % 100 == 0:
                    chart_collector.save_intermediate_data(all_charts, current_date)
                current_date += interval_delta
                pbar.update(1)

        # 최종 데이터 저장
        df = pd.DataFrame(all_charts)
        output_filename = f'melon_charts_{start_date.strftime("%Y%m%d")}_{end_date.strftime("%Y%m%d")}.csv'
        df.to_csv(output_filename, index=False, encoding='utf-8-sig')
        
        # 임시 파일 삭제
        chart_collector.cleanup_temp_files()
        
    except Exception as e:
        logging.error(f"오류 발생: {str(e)}")
    
    return all_charts

if __name__ == "__main__":
    try:
        # 날짜 입력 받기
        while True:
            try:
                # 시작 날짜 입력
                start_date_input = input("시작 날짜를 년도, 월, 일, 시간 순으로 공백으로 구분하여 입력해주세요 (예: 2024 1 1 0): ")
                start_date_list = list(map(int, start_date_input.split()))
                
                # 종료 날짜 입력
                end_date_input = input("종료 날짜를 년도, 월, 일, 시간 순으로 공백으로 구분하여 입력해주세요 (예: 2024 1 7 23): ")
                end_date_list = list(map(int, end_date_input.split()))
                
                # 수집 주기 입력
                interval = input("수집 주기를 입력해주세요 (year/month/day/hour): ")
                if interval.lower() not in ['year', 'month', 'day', 'hour']:
                    print("올바른 수집 주기를 입력해주세요.")
                    continue
                
                # datetime 객체 생성
                start_date = datetime(*start_date_list)
                end_date = datetime(*end_date_list)
                
                if end_date < start_date:
                    print("종료 날짜가 시작 날짜보다 앞설 수 없습니다.")
                    continue
                    
                break
                
            except ValueError:
                print("올바른 날짜 형식으로 입력해주세요.")
                continue
                
        # 입력된 날짜 확인
        print(f"\n데이터 수집 기간:")
        print(f"시작: {start_date.strftime('%Y년 %m월 %d일 %H시')}")
        print(f"종료: {end_date.strftime('%Y년 %m월 %d일 %H시')}")
        print(f"수집 주기: {interval}")
        
        # 사용자 확인
        confirm = input("\n데이터 수집을 시작하시겠습니까? (y/n): ")
        if confirm.lower() == 'y':
            charts = get_charts_for_period(
                start_date=start_date,
                end_date=end_date,
                interval=interval,
                delay_seconds=2
            )
            print("\n데이터 수집이 완료되었습니다.")
        else:
            print("데이터 수집이 취소되었습니다.")
            
    except Exception as e:
        print(f"오류 발생: {str(e)}")
