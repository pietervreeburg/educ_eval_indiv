# Script to create education evaluation reports from exported OWDB data, 2017_10_26
# Pieter Vreeburg, E:vreeburg@ese.eur.nl

# Watch out for
    # CHECKED: The available data is not consistent with regards to ERNA-ids. Sometimes only SAP-id is used (eg 6610),
        # sometimes the full ERNA-id is used (eg 06610pfr) (Use Excel to trim full ERNA-id to SAP-id: =INT(LEFT(A2;5)) )
    # CHECKED: The answer-values can span 1:6 instead of 1:5. 6 is Na. Filter this in the source data (not provisioned for in this script)
    # CHECKED: The selected output format (for stand-alone use or as part of the RO pipeline: use the output_format variable to set the output format)
    # Teacher_score: all questions minus 'Has a good command of the English language'
# v2018
    # GENERAL: Add course scores
    # GENERAL: Add course code (easier to verify reports)
    # GENERAL: Delete tables with no data (see for example Asim in 2017)
    # OS: There should be a question about the students spent on this course. Add this question.
    # AW: Add question regarding 'leeropbrengst' (new for blok 1, 2018)
    # PHF: Experiment with ML / Sentiment analysis approach to open questions (Consult Frasincar / Glorie) Maybe use for final assignment EQI?
    
# cli options
    # --nodata: strip all data from the html file just before printing

# imports
import os # os operations, from std. library
import argparse # command line parsing, from std. library
from collections import OrderedDict # ordered dictionary, from std. library

import pandas as pd # dataframes functionality
import numpy as np # numeric functions for use in pandas
from bs4 import BeautifulSoup # HTML parser, used to add CSS ID to HTML tables
from jinja2 import Environment, FileSystemLoader # templating engine
import pdfkit # to Py wrapper for wkhtmltopdf.exe

# set dirs / files
inputfile = 'INPUT_educ_eval_indiv_report_py.txt'
sourcefile = 'educ_eval_indiv_report_data_2015_2016.xlsx'
main_dir = r'\\campus.eur.nl\shared\departments\ESE-FD-BB-ONDERZOEK\Pieter_Vreeburg\1_Project_support\Educ_eval_indiv_reports'
report_dir = 'reports'
wkhtmltopdf_exe = r'C:\Users\50389pvr\Downloads\no-app-control\wkhtmltopdf\bin\wkhtmltopdf.exe'

# set global options
pd.set_option('display.max_colwidth', -1) # Pandas, no truncation of values
env = Environment(loader = FileSystemLoader(main_dir)) # Jinja
template = env.get_template('educ_eval_indiv_report_template.html') # Jinja
pdfoptions = { # wkhtmltopdf
                'print-media-type' : None, # cli switch without arguments, comment out to switch to screen-media-type
                'page-size' : 'A4',
                'margin-top': '10mm',
                'margin-right': '10mm',
                'margin-bottom': '10mm',
                'margin-left': '10mm',
                'footer-right' : 'Page [page] of [toPage]',
                'footer-left' : 'Report generated on: [date]',
                'footer-font-name' : 'sans-serif', # default Arial
                'footer-font-size' : '8', # default 12
                'quiet': None # cli switch without arguments, comment out to switch to verbose mode
                }
pdfconfiguration = pdfkit.configuration(wkhtmltopdf = wkhtmltopdf_exe)
output_format = 'stand_alone_flat' # switch between 'ro', 'stand_alone' or 'stand_alone_flat' (with the last option files are not sorted into subfolders)

# set up cli options parser
opt_parser = argparse.ArgumentParser()
opt_parser.add_argument('--nodata', help = 'Produce reports with data cells represented as XXX', action = 'store_true')
options = opt_parser.parse_args()

# functions
def html_table_out(dataframe_in, table_id, del_header = None):
    if del_header == 'del_header':
        html = dataframe_in.to_html(index = False, header = False)
    else:
        html = dataframe_in.to_html()
    soup = BeautifulSoup(html, 'lxml')
    soup.find('table')['id'] = table_id
    del soup.table['border']
    html_out = unicode(str(soup), 'utf-8')

    return html_out

def nodata(html_in):
    soup = BeautifulSoup(html_in, 'lxml')
    td_tags = soup.find_all('td')
    for tag in td_tags:
        tag.string = 'XXX'
    html_out = unicode(str(soup), 'utf-8')

    return html_out

# main
def main(options):
    # read Excel source file
    df_teacher_data = pd.read_excel(os.path.join(main_dir, sourcefile), sheetname = 0)
    df_teacher_data = df_teacher_data.rename(columns = {'EVL_SIN_ID' : 'sin_id',
                                                         'EVL_JAAR' : 'course_year',
                                                         'VRG_TEXT_ENG' : 'question_text',
                                                         'EEV_PARAM_DOCENT' : 'teacher_erna',
                                                         'Total' : 'resp_count',
                                                         'AMC_ORDERID' : 'resp_value',
                                                         'Lange naam' : 'course_name'})
    df_teacher_data = df_teacher_data.sort_values(['teacher_erna', 'course_year', 'course_name', 'question_text'], ascending = [True, False, True, True])
    df_open_answers = pd.read_excel(os.path.join(main_dir, sourcefile), sheetname = 1)
    df_open_answers = df_open_answers.rename(columns = {'EVL_SIN_ID' : 'sin_id',
                                                         'VRG_TEXT_ENG' : 'question_text',
                                                         'ROP_CONTENT' : 'resp_value'})
    df_open_answers = df_open_answers.sort_values(['sin_id', 'question_text'])

    # add calculated total_value to df_teacher_data
    df_teacher_data['total_value'] = df_teacher_data['resp_value'] * df_teacher_data['resp_count']

    # create teacher stats pivot
    teacher_stats = df_teacher_data[~df_teacher_data['question_text'].str.contains('good command of the English language')]
    teacher_stats = teacher_stats.groupby(['teacher_erna', 'course_year'])[['resp_count', 'total_value']].sum()
    teacher_stats['teacher_score'] = teacher_stats['total_value'] / teacher_stats['resp_count']
    teacher_stats = teacher_stats.dropna().round(2)
    teacher_stats = teacher_stats.drop(['total_value', 'resp_count'], axis = 1)
    teacher_stats = teacher_stats.sort_index(ascending = False)
    teacher_stats = teacher_stats.rename_axis(['erna id', 'year']).rename(columns = {'teacher_score' : 'teacher score'})

    # create courses details pivot
    courses_details = df_teacher_data.groupby(['teacher_erna', 'course_year', 'course_name', 'question_text'])[['resp_count', 'total_value']].sum()
    courses_details['teacher_score'] = courses_details['total_value'] / courses_details['resp_count']
    courses_details = courses_details.dropna().round(2)
    courses_details = courses_details.drop('total_value', axis = 1)
    courses_details = courses_details.sort_index(level = ['teacher_erna', 'course_year', 'course_name', 'question_text'], ascending = [True, False, True, True])
        # Broken implementation in pandas 0.2, wil be fixed in 0.21 (https://github.com/pandas-dev/pandas/issues/16934)
    courses_details = courses_details.rename_axis(['erna id', 'year', 'course', 'question']).rename(columns = {'resp_count' : 'respondents', 'teacher_score' : 'teacher score'})

    # iterate through input file, find course stats, course details and open answers to create individual reports
    input_file = open(os.path.join(main_dir, inputfile)).read().splitlines()
    list_missing = []
    for line in input_file:
        teacher_erna_id, pers_name, dept, pers_type = line.split(';')
        teacher_erna_id = int(teacher_erna_id)
        print 'processing:', teacher_erna_id
        # teacher stats & courses details
        try:
            teacher_stats_rep = teacher_stats.xs(teacher_erna_id, level = 0)
        except KeyError:
            list_missing.append('{}; {}; {}'.format(teacher_erna_id, pers_name, dept))
            continue
        courses_details_rep = courses_details.xs(teacher_erna_id, level = 0)
        # open questions
        # Format courses_open_answers: [[str_course_year, str_course_name', {str_question_text: [list_open_questions_answers]}], etc.]
        courses_taught = df_teacher_data.loc[(df_teacher_data['teacher_erna'] == teacher_erna_id), ['sin_id', 'course_name', 'course_year']].drop_duplicates()
        courses_index = OrderedDict()
        courses_open_answers = []
        for index, row in courses_taught.iterrows():
            # get open questions data from df_open_answers for this course, try next course if there are no open answers
            try:
                course_features = df_open_answers.loc[(df_open_answers['sin_id'] == row['sin_id'])]
            except KeyError:
                continue
            course_year = row['course_year']
            course_name = row['course_name']
            # update course index for this course (for sidebar)
            courses_index.setdefault(course_year, []).append(course_name)
            open_answer_dict = {}
            # create dict with open questions for this course
            for index, row in course_features.iterrows():
                question_text = row['question_text']
                open_answer_dict.setdefault(question_text, []).append(row['resp_value'])
            # reorder dict with open questions for this course according to sort_list
            sort_list = ['The strongest features of this course are:',
                        'De sterke punten van dit vak zijn:',
                        'The weakest features of this course are:',
                        'De zwakke punten van dit vak zijn:']
            open_answer_dict_sorted = OrderedDict()
            for search_item in sort_list:
                if search_item in open_answer_dict.keys():
                    open_answer_dict_sorted[search_item] = open_answer_dict.get(search_item)
                    del open_answer_dict[search_item]
            open_answer_dict_sorted.update(open_answer_dict)
            # collect all open questions data for this course together
            course_open_answers = []
            course_open_answers.append(course_name)
            course_open_answers.append(course_year)
            course_open_answers.append(open_answer_dict_sorted)
            # add collected open questions data for this course to report
            courses_open_answers.append(course_open_answers)

        # set Jinja template vars and render HTML (with nodata option)
        template_vars = {'name' : pers_name,
                        'erna' : teacher_erna_id,
                        'teacher_statistics' : html_table_out(teacher_stats_rep, 'one-column-emphasis-teacher'),
                        'courses_details' : html_table_out(courses_details_rep, 'one-column-emphasis'),
                        'courses_index' : courses_index,
                        'courses_features' : courses_open_answers
                        }
        html_out = template.render(template_vars)
        if options.nodata:
            html_out = nodata(html_out)

        # set up output
        if options.nodata:
            nodata_string = '_NODATA'
        else:
            nodata_string = ''
        if output_format == 'stand_alone':
            filename_string = os.path.join(dept, 'Educ_eval_{}_{}_{}{}'.format(pers_name.replace(' ', '_'), teacher_erna_id, dept, nodata_string))
            if not os.path.isdir(os.path.join(main_dir, report_dir, dept)):
                os.mkdir(os.path.join(main_dir, report_dir, dept))
        elif output_format == 'stand_alone_flat':
            filename_string = 'Educ_eval_{}_{}_{}{}'.format(pers_name.replace(' ', '_'), teacher_erna_id, dept, nodata_string)
        elif output_format == 'ro':
            filename_string = '{}{}'.format(teacher_erna_id, nodata_string)
        else:
            print 'No output format selected, exiting!'
            exit()
        # output PDF
        pdfkit.from_string(html_out, os.path.join(main_dir, report_dir, filename_string + '.pdf'), options = pdfoptions, configuration = pdfconfiguration)
        # output HTML
        with open(os.path.join(main_dir, report_dir, filename_string + '.html'), 'w') as file_out:
            file_out.write(html_out.encode('utf-8'))

    # write simple log
    with open(os.path.join(main_dir, 'LOG_missing_educ_eval_indiv_report.txt'), 'w') as f_out:
        for item in list_missing:
            line = item + '\n'
            f_out.write(line)

    print 'Done'

if __name__ == '__main__':
    main(options)

# USEFUL CODE SNIPPETS
    # temporary HTML writer for testing
    # output = df_teacher_data
    # with open(os.path.join(main_dir, 'test_output.html'), 'w') as html_out:
        # html_out.write(output.to_html().encode('utf-8'))
    # exit()