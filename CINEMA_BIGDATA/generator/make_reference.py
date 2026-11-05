"""Sinh danh mục có seed, mỗi phòng có nhiều suất không chồng thời gian."""
import csv
import os
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

DATA = Path(os.getenv('DATA_DIR', '/data'))

def write_csv(name, rows):
    target = DATA / 'reference' / name
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open('w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)

def make_reference():
    rng = random.Random(int(os.getenv('SEED', '42')))
    base = datetime.fromisoformat(os.getenv('BASE_TIME', '2026-10-25T07:00:00+00:00'))
    movies = [dict(movie_id=f'M{i:03}', title=f'Phim {i}', genre=['action','comedy','drama'][i%3], duration_min=100, release_date=base.date().isoformat()) for i in range(100)]
    cinemas = [dict(cinema_id=f'C{i:02}', name=f'Rap {i}', city=['HCM','Hanoi','Danang'][i%3]) for i in range(20)]
    rooms = [dict(room_id=f'R{i:03}', cinema_id=f'C{i//20:02}', seat_count=rng.randrange(80,201,10)) for i in range(400)]
    shows = []
    for i in range(50000):
        position = i * 60000 // 50000
        day = position // (400*5)
        slot = (position // 400) % 5
        start = base.replace(hour=2, minute=0, second=0) + timedelta(days=day, hours=slot*3)
        shows.append(dict(showtime_id=f'S{i:06}', movie_id=f'M{rng.randrange(100):03}', room_id=rooms[position%400]['room_id'], start_time=start.isoformat(), base_price=rng.choice([75000,95000,120000])))
    for name, rows in [('movies.csv', movies),('cinemas.csv',cinemas),('rooms.csv',rooms),('showtimes.csv',shows)]:
        write_csv(name, rows)
    print({'movies':len(movies), 'cinemas':len(cinemas), 'rooms':len(rooms), 'showtimes':len(shows)})

if __name__ == '__main__':
    make_reference()
