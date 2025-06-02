"""
豆瓣Top250电影爬虫项目
作者: [赵嘉恒]
日期: 2025年5月
"""

import os
import time
import random
import requests
from bs4 import BeautifulSoup
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from wordcloud import WordCloud
import jieba
import logging
from tqdm import tqdm
import re

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("douban_crawler.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DoubanMovieCrawler:
    """豆瓣电影Top250爬虫类"""
    
    def __init__(self):
        self.base_url = "https://movie.douban.com/top250"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Connection': 'keep-alive',
            'Referer': 'https://movie.douban.com/'
        }
        self.movies = []
        
    def get_user_agents(self):
        """返回一个User-Agent列表"""
        user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:90.0) Gecko/20100101 Firefox/90.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 11.5; rv:90.0) Gecko/20100101 Firefox/90.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 11_5_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.2 Safari/605.1.15'
        ]
        return user_agents
    
    def get_random_header(self):
        """获取随机的请求头"""
        user_agents = self.get_user_agents()
        headers = self.headers.copy()
        headers['User-Agent'] = random.choice(user_agents)
        return headers
        
    def crawl_page(self, page):
        """爬取指定页面的电影数据
        
        Args:
            page: 页码
            
        Returns:
            成功爬取的电影数量
        """
        try:
            url = f"{self.base_url}?start={(page-1)*25}&filter="
            logger.info(f"爬取页面: {url}")
            
            headers = self.get_random_header()
            response = requests.get(url, headers=headers, timeout=10)
            
            if response.status_code != 200:
                logger.error(f"请求失败: {response.status_code}")
                return 0
            
            # 随机延迟，防止被封IP
            time.sleep(random.uniform(1, 3))
            
            soup = BeautifulSoup(response.text, "lxml")
            movie_list = soup.select("div.article ol.grid_view li")
            
            if not movie_list:
                logger.error("未找到电影列表，可能是页面结构变化或遇到反爬机制")
                # 保存当前页面以便调试
                with open(f"error_page_{page}.html", "w", encoding="utf-8") as f:
                    f.write(response.text)
                return 0
            
            movies_found = 0
            for movie in movie_list:
                try:
                    # 更加健壮的电影信息提取
                    # 电影标题 - 使用更加健壮的选择器
                    title_element = movie.select_one("div.hd a span.title")
                    if title_element is None:
                        logger.warning("无法找到电影标题元素，跳过")
                        continue
                    
                    title = title_element.text.strip()
                    
                    # 获取其他信息 - 使用更加健壮的方式
                    info_elements = movie.select("div.bd p")
                    if not info_elements:
                        logger.warning(f"电影'{title}'无法找到详情信息元素，跳过")
                        continue
                    
                    info = info_elements[0].get_text(strip=True, separator='\n')
                    info_lines = info.split('\n')
                    
                    # 默认值
                    director = "未知"
                    year = "未知"
                    country = "未知"
                    movie_type = "未知"
                    
                    # 导演、主演信息通常在第一行
                    if len(info_lines) > 0:
                        director_actors = info_lines[0].strip()
                        director_match = re.search(r'导演:([^主演]*)(?:主演:|$)', director_actors)
                        if director_match:
                            director = director_match.group(1).strip()
                    
                    # 年份、国家、类型信息通常在第二行
                    if len(info_lines) > 1:
                        year_country_type = info_lines[1].strip()
                        
                        # 提取年份
                        year_match = re.search(r'(\d{4})', year_country_type)
                        if year_match:
                            year = year_match.group(1)
                        
                        # 尝试提取国家/地区
                        country_match = re.search(r'(\d{4})([^/]*)', year_country_type)
                        if country_match:
                            country = country_match.group(2).strip()
                        
                        # 提取类型
                        type_parts = year_country_type.split('/')
                        if len(type_parts) > 1:
                            movie_type = type_parts[-1].strip()
                    
                    # 提取评分 - 使用更加健壮的选择器
                    rating_element = movie.select_one("div.bd div.star span.rating_num")
                    rating = rating_element.text.strip() if rating_element else "0.0"
                    
                    # 提取评价人数 - 使用更加健壮的选择器和正则表达式
                    rating_count = "0"
                    rating_count_elements = movie.select("div.bd div.star span")
                    for element in rating_count_elements:
                        count_match = re.search(r'(\d+)人评价', element.text)
                        if count_match:
                            rating_count = count_match.group(1)
                            break
                    
                    # 提取一句话简评 - 使用更加健壮的选择器
                    quote = ""
                    quote_element = movie.select_one("div.bd p.quote span.inq")
                    if quote_element:
                        quote = quote_element.text.strip()
                    
                    movie_data = {
                        "rank": len(self.movies) + 1,
                        "title": title,
                        "director": director,
                        "year": year,
                        "country": country,
                        "type": movie_type,
                        "rating": rating,
                        "rating_count": rating_count,
                        "quote": quote
                    }
                    
                    self.movies.append(movie_data)
                    logger.debug(f"成功爬取电影: {title}")
                    movies_found += 1
                    
                except Exception as e:
                    logger.error(f"解析电影数据时出错: {e}")
            
            return movies_found
            
        except Exception as e:
            logger.error(f"爬取页面出错: {e}")
            return 0
    
    def crawl(self):
        """爬取豆瓣Top250电影"""
        total_pages = 10  # 豆瓣电影Top250共10页
        
        try:
            for page in tqdm(range(1, total_pages + 1), desc="爬取进度"):
                count = self.crawl_page(page)
                logger.info(f"第{page}页爬取完成，获取{count}部电影")
                
                if count == 0:
                    logger.warning(f"第{page}页未获取到任何电影，尝试重试...")
                    # 增加更长的等待时间并重试
                    time.sleep(random.uniform(10, 15))
                    count = self.crawl_page(page)
                    if count == 0:
                        logger.error(f"重试后仍未获取到电影，可能遇到反爬机制，暂停一段时间")
                        time.sleep(random.uniform(30, 60))
                
                # 随机暂停一段时间，避免请求过于频繁
                time.sleep(random.uniform(5, 8))
                
            logger.info(f"爬取完成，总共获取{len(self.movies)}部电影")
            
            # 如果获取的电影数量太少，可能是遇到了反爬机制
            if len(self.movies) < 50:  # 预期是250部，如果少于50可能有问题
                logger.warning(f"获取的电影数量({len(self.movies)})过少，可能遇到了反爬机制")
            
            return self.movies
        
        except Exception as e:
            logger.error(f"爬取过程中出现错误: {e}")
            return self.movies

    def save_to_excel(self, filename="output/movies.xlsx"):
        """将电影数据保存到Excel文件
        
        Args:
            filename: 保存的文件名
        """
        try:
            # 确保输出目录存在
            os.makedirs(os.path.dirname(filename), exist_ok=True)
            
            # 如果没有数据，给出提示
            if not self.movies:
                logger.error("没有电影数据可保存")
                return False
            
            # 创建DataFrame
            df = pd.DataFrame(self.movies)
            
            # 保存到Excel
            df.to_excel(filename, index=False)
            logger.info(f"数据已保存到 {filename}")
            
            # 同时保存为CSV格式，作为备份
            csv_filename = filename.replace('.xlsx', '.csv')
            df.to_csv(csv_filename, index=False, encoding='utf-8-sig')
            logger.info(f"数据已备份到 {csv_filename}")
            
            return True
        except Exception as e:
            logger.error(f"保存数据出错: {e}")
            return False

    def simulate_human_behavior(self):
        """模拟人类行为，减少被反爬机制识别的可能性"""
        # 随机访问一些非目标页面
        try:
            random_pages = [
                "https://movie.douban.com/",
                "https://movie.douban.com/cinema/nowplaying/",
                "https://movie.douban.com/explore"
            ]
            
            page = random.choice(random_pages)
            logger.info(f"模拟人类行为，随机访问: {page}")
            
            headers = self.get_random_header()
            requests.get(page, headers=headers, timeout=10)
            
            # 随机等待
            time.sleep(random.uniform(2, 5))
            
        except Exception as e:
            logger.error(f"模拟人类行为时出错: {e}")


class DataAnalyzer:
    """数据分析类"""
    
    def __init__(self, data):
        """
        初始化数据分析器
        
        Args:
            data: DataFrame或包含电影数据的文件路径
        """
        if isinstance(data, str):
            # 尝试不同的方式加载数据
            try:
                if data.endswith('.xlsx'):
                    self.df = pd.read_excel(data)
                elif data.endswith('.csv'):
                    self.df = pd.read_csv(data)
                else:
                    logger.error(f"不支持的文件格式: {data}")
                    raise ValueError(f"不支持的文件格式: {data}")
            except Exception as e:
                logger.error(f"加载数据文件出错: {e}")
                raise
        else:
            self.df = data
        
        # 检查数据是否为空
        if self.df.empty:
            logger.error("数据为空，无法进行分析")
            raise ValueError("数据为空，无法进行分析")
        
        # 数据预处理
        self._preprocess()
    
    def _preprocess(self):
        """预处理数据"""
        try:
            # 检查必要的列是否存在
            required_columns = ['rating', 'rating_count', 'year', 'type']
            for col in required_columns:
                if col not in self.df.columns:
                    logger.error(f"缺少必要的列: {col}")
                    raise ValueError(f"缺少必要的列: {col}")
            
            # 处理缺失值
            self.df['quote'] = self.df['quote'].fillna('')
            
            # 转换评分为浮点型
            self.df['rating'] = pd.to_numeric(self.df['rating'], errors='coerce').fillna(0).astype(float)
            
            # 转换评价人数为整型
            self.df['rating_count'] = pd.to_numeric(self.df['rating_count'], errors='coerce').fillna(0).astype(int)
            
            # 转换年份为整型
            self.df['year'] = pd.to_numeric(self.df['year'], errors='coerce').fillna(0).astype(int)
            
            # 处理电影类型
            self.df['type_list'] = self.df['type'].fillna('').astype(str).str.split('/')
            
            # 提取出所有类型
            all_types = []
            for types in self.df['type_list']:
                if isinstance(types, list):
                    all_types.extend([t.strip() for t in types if t.strip()])
            
            self.all_types = all_types
            
        except Exception as e:
            logger.error(f"数据预处理出错: {e}")
            raise
    
    def year_distribution(self, output_file="output/images/year_distribution.png"):
        """分析电影年份分布"""
        try:
            plt.figure(figsize=(12, 6))
            
            # 过滤掉无效年份
            valid_years = self.df[self.df['year'] > 1900]['year']
            if valid_years.empty:
                logger.error("没有有效的年份数据")
                return pd.Series()
            
            year_counts = valid_years.value_counts().sort_index()
            year_counts.plot(kind='bar', color='skyblue')
            plt.title('豆瓣Top250电影年份分布')
            plt.xlabel('年份')
            plt.ylabel('电影数量')
            plt.xticks(rotation=90)
            plt.grid(axis='y', linestyle='--', alpha=0.7)
            plt.tight_layout()
            
            # 保存图片
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            plt.savefig(output_file)
            plt.close()
            
            return year_counts
        except Exception as e:
            logger.error(f"生成年份分布图出错: {e}")
            plt.close()
            return pd.Series()
    
    def rating_distribution(self, output_file="output/images/rating_distribution.png"):
        """分析电影评分分布"""
        try:
            plt.figure(figsize=(10, 6))
            sns.histplot(self.df['rating'], bins=20, kde=True)
            plt.title('豆瓣Top250电影评分分布')
            plt.xlabel('评分')
            plt.ylabel('电影数量')
            plt.grid(axis='y', linestyle='--', alpha=0.7)
            plt.tight_layout()
            
            # 保存图片
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            plt.savefig(output_file)
            plt.close()
            
            return self.df['rating'].describe()
        except Exception as e:
            logger.error(f"生成评分分布图出错: {e}")
            plt.close()
            return None
    
    def country_distribution(self, top_n=10, output_file="output/images/country_distribution.png"):
        """分析电影国家/地区分布"""
        try:
            # 提取所有国家/地区
            all_countries = []
            for country in self.df['country']:
                if isinstance(country, str) and country != "未知":
                    countries = [c.strip() for c in country.split('/') if c.strip()]
                    all_countries.extend(countries)
            
            if not all_countries:
                logger.error("没有有效的国家/地区数据")
                return pd.Series()
            
            # 统计各国家/地区电影数量
            country_counts = pd.Series(all_countries).value_counts()
            
            # 检查是否有足够的数据进行绘图
            if len(country_counts) == 0:
                logger.error("国家/地区数据统计为空")
                return pd.Series()
            
            # 调整top_n，确保不超过实际数据量
            top_n = min(top_n, len(country_counts))
            
            # 绘制饼图
            plt.figure(figsize=(12, 8))
            country_counts[:top_n].plot(kind='pie', autopct='%1.1f%%')
            plt.title(f'豆瓣Top250电影国家/地区分布 (Top {top_n})')
            plt.ylabel('')
            plt.tight_layout()
            
            # 保存图片
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            plt.savefig(output_file)
            plt.close()
            
            return country_counts
        except Exception as e:
            logger.error(f"生成国家分布图出错: {e}")
            plt.close()
            return pd.Series()
    
    def type_distribution(self, output_file="output/images/type_distribution.png"):
        """分析电影类型分布"""
        try:
            if not self.all_types:
                logger.error("没有有效的电影类型数据")
                return pd.Series()
            
            # 统计各类型电影数量
            type_counts = pd.Series(self.all_types).value_counts()
            
            # 绘制条形图
            plt.figure(figsize=(12, 6))
            type_counts.plot(kind='bar', color='lightgreen')
            plt.title('豆瓣Top250电影类型分布')
            plt.xlabel('电影类型')
            plt.ylabel('出现次数')
            plt.xticks(rotation=45)
            plt.grid(axis='y', linestyle='--', alpha=0.7)
            plt.tight_layout()
            
            # 保存图片
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            plt.savefig(output_file)
            plt.close()
            
            return type_counts
        except Exception as e:
            logger.error(f"生成类型分布图出错: {e}")
            plt.close()
            return pd.Series()
    
    def rating_by_year(self, output_file="output/images/rating_by_year.png"):
        """分析不同年代电影评分情况"""
        try:
            # 过滤无效年份
            valid_df = self.df[(self.df['year'] > 1900) & (self.df['rating'] > 0)]
            if valid_df.empty:
                logger.error("没有有效的年份和评分数据")
                return pd.Series()
            
            # 创建年代标签
            valid_df['decade'] = (valid_df['year'] // 10) * 10
            
            # 计算各年代平均评分
            decade_rating = valid_df.groupby('decade')['rating'].mean()
            
            if decade_rating.empty:
                logger.error("年代评分统计为空")
                return pd.Series()
            
            # 绘制折线图
            plt.figure(figsize=(12, 6))
            decade_rating.plot(marker='o')
            plt.title('豆瓣Top250电影各年代平均评分')
            plt.xlabel('年代')
            plt.ylabel('平均评分')
            plt.grid(linestyle='--', alpha=0.7)
            plt.tight_layout()
            
            # 保存图片
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            plt.savefig(output_file)
            plt.close()
            
            return decade_rating
        except Exception as e:
            logger.error(f"生成年代评分图出错: {e}")
            plt.close()
            return pd.Series()
    
    def director_ranking(self, top_n=10, output_file="output/images/director_ranking.png"):
        """分析导演作品数量排名"""
        try:
            # 提取所有导演
            all_directors = []
            for director in self.df['director']:
                if isinstance(director, str) and director != "未知":
                    directors = [d.strip() for d in director.split('/') if d.strip()]
                    all_directors.extend(directors)
            
            if not all_directors:
                logger.error("没有有效的导演数据")
                return pd.Series()
            
            # 统计各导演作品数量
            director_counts = pd.Series(all_directors).value_counts()
            
            if director_counts.empty:
                logger.error("导演作品统计为空")
                return pd.Series()
            
            # 获取前N名导演
            top_n = min(top_n, len(director_counts))
            top_directors = director_counts[:top_n]
            
            # 绘制水平条形图
            plt.figure(figsize=(10, 8))
            top_directors.plot(kind='barh', color='orange')
            plt.title(f'豆瓣Top250电影导演作品数量排名 (Top {top_n})')
            plt.xlabel('作品数量')
            plt.ylabel('导演')
            plt.grid(axis='x', linestyle='--', alpha=0.7)
            plt.tight_layout()
            
            # 保存图片
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            plt.savefig(output_file)
            plt.close()
            
            return top_directors
        except Exception as e:
            logger.error(f"生成导演排名图出错: {e}")
            plt.close()
            return pd.Series()
    
    def generate_wordcloud(self, output_file="output/images/quote_wordcloud.png"):
        """生成电影简评词云"""
        try:
            # 合并所有简评
            all_quotes = ' '.join(self.df['quote'].dropna())
            
            if not all_quotes.strip():
                logger.error("没有有效的简评数据")
                return False
            
            # 使用jieba分词
            words = ' '.join(jieba.cut(all_quotes))
            
            # 指定字体路径
            # 在不同系统上查找合适的中文字体
            font_paths = [
                '/usr/share/fonts/truetype/wqy/wqy-microhei.ttc',  # Linux
                '/System/Library/Fonts/PingFang.ttc',  # macOS
                'C:/Windows/Fonts/simhei.ttf',  # Windows
                'simhei.ttf'  # 当前目录
            ]
            
            font_path = None
            for path in font_paths:
                if os.path.exists(path):
                    font_path = path
                    break
            
            if not font_path:
                logger.warning("未找到中文字体文件，词云可能无法正确显示中文")
                # 创建一个空的字体文件作为备用
                with open('simhei.ttf', 'wb') as f:
                    f.write(b'')
                font_path = 'simhei.ttf'
            
            # 生成词云
            wordcloud = WordCloud(
                font_path=font_path,
                width=800,
                height=600,
                background_color='white',
                max_words=100
            ).generate(words)
            
            # 显示词云
            plt.figure(figsize=(10, 8))
            plt.imshow(wordcloud, interpolation='bilinear')
            plt.axis('off')
            
            # 保存图片
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            plt.savefig(output_file)
            plt.close()
            
            return True
        except Exception as e:
            logger.error(f"生成词云出错: {e}")
            plt.close()
            return False
    
    def generate_report(self):
        """生成数据分析报告"""
        try:
            report = {}
            
            # 基本统计信息
            report['movie_count'] = len(self.df)
            report['avg_rating'] = self.df['rating'].mean()
            report['min_rating'] = self.df['rating'].min()
            report['max_rating'] = self.df['rating'].max()
            
            # 年份分布
            valid_year_df = self.df[self.df['year'] > 1900]
            if not valid_year_df.empty:
                oldest_idx = valid_year_df['year'].idxmin()
                newest_idx = valid_year_df['year'].idxmax()
                
                report['oldest_movie'] = {
                    'title': self.df.loc[oldest_idx, 'title'],
                    'year': self.df.loc[oldest_idx, 'year']
                }
                
                report['newest_movie'] = {
                    'title': self.df.loc[newest_idx, 'title'],
                    'year': self.df.loc[newest_idx, 'year']
                }
                
                report['most_common_year'] = valid_year_df['year'].value_counts().idxmax()
            else:
                report['oldest_movie'] = {'title': '未知', 'year': '未知'}
                report['newest_movie'] = {'title': '未知', 'year': '未知'}
                report['most_common_year'] = '未知'
            
            # 评分分布
            report['rating_counts'] = self.df['rating'].value_counts().sort_index()
            
            # 其他分析
            report['type_counts'] = pd.Series(self.all_types).value_counts() if self.all_types else pd.Series()
            
            return report
        except Exception as e:
            logger.error(f"生成分析报告出错: {e}")
            return {
                'movie_count': len(self.df),
                'error': str(e)
            }


def main():
    """主函数：爬取豆瓣Top250电影并进行数据分析"""
    try:
        # 创建输出目录
        os.makedirs('output/images', exist_ok=True)
        
        # 创建爬虫实例
        crawler = DoubanMovieCrawler()
        
        # 进行一些前置操作，模拟人类行为
        crawler.simulate_human_behavior()
        
        # 爬取电影数据
        movies = crawler.crawl()
        
        # 检查是否获取到了电影数据
        if not movies:
            logger.error("未获取到任何电影数据，程序终止")
            return
        
        # 保存数据到Excel
        if not crawler.save_to_excel():
            logger.error("保存数据失败，程序终止")
            return
        
        # 数据分析
        try:
            analyzer = DataAnalyzer('output/movies.xlsx')
            
            # 生成各类分析图表
            analyzer.year_distribution()
            analyzer.rating_distribution()
            analyzer.country_distribution()
            analyzer.type_distribution()
            analyzer.rating_by_year()
            analyzer.director_ranking()
            analyzer.generate_wordcloud()
            
            # 生成分析报告
            report = analyzer.generate_report()
            
            # 输出分析结果
            print("\n==== 豆瓣TOP250电影数据分析报告 ====")
            print(f"共收集了 {report['movie_count']} 部电影")
            print(f"平均评分: {report['avg_rating']:.2f}")
            print(f"最高评分: {report['max_rating']}")
            print(f"最低评分: {report['min_rating']}")
            
            if isinstance(report['oldest_movie'], dict):
                print(f"最早的电影: {report['oldest_movie']['title']} ({report['oldest_movie']['year']})")
                print(f"最新的电影: {report['newest_movie']['title']} ({report['newest_movie']['year']})")
            else:
                print(f"最早的电影: {report['oldest_movie']['title'].values[0]} ({report['oldest_movie']['year'].values[0]})")
                print(f"最新的电影: {report['newest_movie']['title'].values[0]} ({report['newest_movie']['year'].values[0]})")
            
            print(f"电影最多的年份: {report['most_common_year']}")
            print("\n分析图表已保存到 output/images/ 目录")
        except Exception as e:
            print(f"\n❌ 数据分析或图表生成失败，错误信息: {e}")
    except Exception as e:
        print(f"\n❌ 程序运行失败，错误信息: {e}")

if __name__ == "__main__":
    main()
