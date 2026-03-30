"""Create / upgrade app_scraper_config table in gurysk_app with seed data."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.config import get_database_backend
from src.database import create_db_connection
from src.db_compat import get_dict_cursor, normalize_rows


def main():
    backend = get_database_backend()
    config_db = None if backend == "postgresql" else "gurysk_app"
    config_table = "gurysk_app.app_scraper_config" if backend == "postgresql" else "app_scraper_config"
    conn = create_db_connection(database=config_db)

    if backend == "postgresql":
        ddl = """
        CREATE SCHEMA IF NOT EXISTS gurysk_app;

        CREATE TABLE IF NOT EXISTS gurysk_app.app_scraper_config (
            id BIGSERIAL PRIMARY KEY,
            config_type VARCHAR(32) NOT NULL,
            config_key VARCHAR(128) NOT NULL,
            config_value TEXT,
            schedule_profile VARCHAR(16) NOT NULL DEFAULT 'daily'
                CHECK (schedule_profile IN ('daily', 'weekly', 'both')),
            countries VARCHAR(512) NOT NULL DEFAULT 'US',
            pages INTEGER NOT NULL DEFAULT 1,
            is_active SMALLINT NOT NULL DEFAULT 1
                CHECK (is_active IN (0, 1)),
            description VARCHAR(255),
            user_id VARCHAR(64),
            create_timestamp TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            update_timestamp TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT uk_type_key_schedule UNIQUE (config_type, config_key, schedule_profile)
        );
        """

        seed = """
        INSERT INTO gurysk_app.app_scraper_config
            (config_type, config_key, config_value, schedule_profile, countries, pages, is_active, description, user_id)
        VALUES
            ('product_query','outin',NULL,'daily','US',1,1,'Outin品牌关键词搜索','system'),
            ('category_query','coffee machines',NULL,'weekly','US',3,1,'咖啡机大类关键词搜索','system'),
            ('category_query','portable coffee maker',NULL,'weekly','US',3,1,'便携咖啡机细分搜索','system'),
            ('target_asin','B0BRKFWPF3',NULL,'daily','US',1,1,'Outin Nano - 核心单品跟踪','system'),
            ('brand_search','OUTIN','{"brand":"OUTIN","query":"coffee maker"}','daily','US',2,1,'OUTIN品牌搜索(带brand过滤)','system'),
            ('category_scan','289745','{"category_name":"Coffee Machines"}','daily','US',3,1,'Home & Kitchen > Kitchen & Dining > Coffee Machines 类目扫描','system'),
            ('segment_scan','portable coffee machine','{"segment_name":"Portable Coffee Machines"}','daily','US',2,1,'自定义细分市场：便携咖啡机','system'),
            ('bestseller_scan','kitchen/coffee-machines',NULL,'weekly','US',1,1,'咖啡机Best Seller排名','system'),
            ('review_scan','B0BRKFWPF3',NULL,'weekly','US',1,1,'Outin Nano Top Reviews采集','system'),
            ('offer_scan','B0BRKFWPF3',NULL,'daily','US',1,1,'Outin Nano 多卖家报价监控','system'),
            ('setting','request_delay','1','both','US',1,1,'API请求间隔(秒)','system'),
            ('setting','daily_schedule_time','08:00','daily','US',1,1,'每日采集时间(CST)','system'),
            ('setting','weekly_schedule_day','monday','weekly','US',1,1,'每周采集执行日','system')
        ON CONFLICT (config_type, config_key, schedule_profile) DO UPDATE SET
            config_value = EXCLUDED.config_value,
            countries = EXCLUDED.countries,
            pages = EXCLUDED.pages,
            is_active = EXCLUDED.is_active,
            description = EXCLUDED.description,
            user_id = EXCLUDED.user_id,
            update_timestamp = CURRENT_TIMESTAMP
        """
    else:
        ddl = """
        CREATE TABLE IF NOT EXISTS app_scraper_config (
            id INT AUTO_INCREMENT PRIMARY KEY,
            config_type VARCHAR(32) NOT NULL
                COMMENT 'product_query / category_query / target_asin / brand_search / category_scan / segment_scan / bestseller_scan / review_scan / offer_scan / setting',
            config_key VARCHAR(128) NOT NULL
                COMMENT '关键词/ASIN/category_id/品牌名/设置项名称',
            config_value TEXT
                COMMENT '设置项的值 或 附加参数 JSON',
            schedule_profile ENUM('daily','weekly','both') NOT NULL DEFAULT 'daily'
                COMMENT 'daily=每日, weekly=每周, both=两者',
            countries VARCHAR(512) NOT NULL DEFAULT 'US'
                COMMENT '适用市场(ALL或逗号分隔国家码)',
            pages INT NOT NULL DEFAULT 1
                COMMENT '搜索/类目页数',
            is_active TINYINT(1) NOT NULL DEFAULT 1
                COMMENT '1=启用, 0=停用',
            description VARCHAR(255)
                COMMENT '配置说明',
            user_id VARCHAR(64)
                COMMENT '创建/修改者ID',
            create_timestamp DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            update_timestamp DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uk_type_key_schedule (config_type, config_key, schedule_profile)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        COMMENT='电商采集配置表 - 当前以 Amazon 数据源为主，支持多品牌多品类扩展'
        """

        seed = """
        INSERT INTO app_scraper_config
            (config_type, config_key, config_value, schedule_profile, countries, pages, is_active, description, user_id)
        VALUES
            ('product_query','outin',NULL,'daily','US',1,1,'Outin品牌关键词搜索','system'),
            ('category_query','coffee machines',NULL,'weekly','US',3,1,'咖啡机大类关键词搜索','system'),
            ('category_query','portable coffee maker',NULL,'weekly','US',3,1,'便携咖啡机细分搜索','system'),
            ('target_asin','B0BRKFWPF3',NULL,'daily','US',1,1,'Outin Nano - 核心单品跟踪','system'),
            ('brand_search','OUTIN','{"brand":"OUTIN","query":"coffee maker"}','daily','US',2,1,'OUTIN品牌搜索(带brand过滤)','system'),
            ('category_scan','289745','{"category_name":"Coffee Machines"}','daily','US',3,1,'Home & Kitchen > Kitchen & Dining > Coffee Machines 类目扫描','system'),
            ('segment_scan','portable coffee machine','{"segment_name":"Portable Coffee Machines"}','daily','US',2,1,'自定义细分市场：便携咖啡机','system'),
            ('bestseller_scan','kitchen/coffee-machines',NULL,'weekly','US',1,1,'咖啡机Best Seller排名','system'),
            ('review_scan','B0BRKFWPF3',NULL,'weekly','US',1,1,'Outin Nano Top Reviews采集','system'),
            ('offer_scan','B0BRKFWPF3',NULL,'daily','US',1,1,'Outin Nano 多卖家报价监控','system'),
            ('setting','request_delay','1','both','US',1,1,'API请求间隔(秒)','system'),
            ('setting','daily_schedule_time','08:00','daily','US',1,1,'每日采集时间(CST)','system'),
            ('setting','weekly_schedule_day','monday','weekly','US',1,1,'每周采集执行日','system')
        ON DUPLICATE KEY UPDATE
            config_value=VALUES(config_value),
            countries=VALUES(countries),
            pages=VALUES(pages),
            is_active=VALUES(is_active),
            description=VALUES(description),
            user_id=VALUES(user_id),
            update_timestamp=CURRENT_TIMESTAMP
        """

    with conn.cursor() as cur:
        cur.execute(ddl)
        cur.execute(seed)
    conn.commit()

    with get_dict_cursor(conn) as cur:
        cur.execute(
            "SELECT id, config_type, config_key, config_value, schedule_profile, "
            "countries, pages, is_active, description, user_id "
            f"FROM {config_table} ORDER BY config_type, id"
        )
        rows = normalize_rows(cur.fetchall())
        print("Table created with %d rows:" % len(rows))
        for r in rows:
            print("  ", r)

    conn.close()


if __name__ == "__main__":
    main()
