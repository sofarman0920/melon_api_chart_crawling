import time
import json
import logging
from datetime import datetime, timedelta
from melon import ChartData
from typing import List, Dict
from tqdm import tqdm

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class MelonChartHistory:
    def __init__(self, imageSize: int = 256, delay_seconds: int = 2):
        self.imageSize = imageSize
        self.delay_seconds = delay_seconds

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

def save_intermediate_data(data: List[Dict], current_date: datetime):
    """중간 데이터 저장"""
    filename = f"chart_data_{current_date.strftime('%Y%m%d_%H%M')}.json"
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        logging.info(f"중간 데이터 저장 완료: {filename}")
    except Exception as e:
        logging.error(f"데이터 저장 실패: {str(e)}")

def get_charts_for_period(start_date: datetime, end_date: datetime, delay_seconds: int = 2) -> List[Dict]:
    """기간별 차트 데이터를 수집합니다."""
    all_charts = []
    total_hours = int((end_date - start_date).total_seconds() / 3600) + 1
    
    chart_collector = MelonChartHistory(delay_seconds=delay_seconds)
    
    try:
        current_date = start_date
        with tqdm(total=total_hours, desc="차트 데이터 수집", unit="hour") as pbar:
            while current_date <= end_date:
                daily_chart = chart_collector.get_chart_by_date(current_date)
                all_charts.extend(daily_chart)
                
                if len(all_charts) % 100 == 0:
                    save_intermediate_data(all_charts, current_date)
                    
                current_date += timedelta(hours=1)
                pbar.update(1)
                
    except Exception as e:
        logging.error(f"오류 발생: {str(e)}")
        
    return all_charts
