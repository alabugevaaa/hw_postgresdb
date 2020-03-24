# coding: utf-8
import psycopg2 as pg
from pprint import pprint


def create_db():  # создает таблицы
    with conn.cursor() as cur:
        cur.execute("""
        create table if not exists student(
            id serial primary key,
            name varchar(100) not null,
            gpa numeric(10,2),
            birth timestamp with time zone
        )
        """)

        cur.execute("""
        create table if not exists course(
            id serial primary key,
            name varchar(100) not null
        )
        """)

        cur.execute("""
        create table if not exists student_course(
            id serial primary key,
            student_id integer references student(id),
            course_id integer references course(id)
        )
        """)


def add_course(name):
    with conn.cursor() as cur:
        cur.execute("insert into course(name) values(%s) RETURNING id", (name,))
        return int(cur.fetchone()[0])


def get_students(course_id):  # возвращает студентов определенного курса
    with conn.cursor() as cur:
        cur.execute("""
        select s.* from student as s
            join student_course as sc
                on s.id = sc.student_id
                and sc.course_id = %s
        """, (course_id,))
        pprint(cur.fetchall())


def add_students(course_id, students):  # создает студентов и  записывает их на курс
    for student in students:
        student_id = add_student(student)
        with conn.cursor() as cur:
            cur.execute("insert into student_course(student_id, course_id) values(%s, %s)", (student_id, course_id))


def add_student(student):  # просто создает студента
    with conn.cursor() as cur:
        cur.execute("insert into student(name, gpa, birth) values(%s, %s, %s) RETURNING id",
                    (student['name'], student['gpa'], student['birth']))
        return int(cur.fetchone()[0])


def get_student(student_id):
    with conn.cursor() as cur:
        cur.execute("select * from student where id = %s", (student_id,))
        pprint(cur.fetchone())


if __name__ == '__main__':
    with pg.connect(dbname="netology", user="test", password="1234") as conn:
        create_db()

        student = {'name': 'Романов Роман',
                   'gpa': 4.8,
                   'birth': '2001-01-01'}
        student_id = add_student(student)
        get_student(student_id)

        course_id = add_course('Макраме')
        students = [{'name': 'Литвинцева Юлия',
                     'gpa': 5,
                     'birth': '2001-05-20'},
                    {'name': 'Синичкина Анна',
                     'gpa': 4.5,
                     'birth': '2002-08-25'
                    }]
        add_students(course_id, students)
        get_students(course_id)
