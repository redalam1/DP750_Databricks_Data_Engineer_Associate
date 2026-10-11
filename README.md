# Databricks Learning Project

## 📚 Project Overview

This project is designed to support preparation for the **Databricks Certified Data Engineer Professional (DP-750)** exam through structured learning materials and multiple-choice quizzes (MCQs).

Each course covers a key concept in the Databricks ecosystem. An associated JSON file contains questions to assess understanding and reinforce knowledge of the topics covered.

The web interface, `quiz.html`, allows users to load quizzes, answer questions with a timer, and receive immediate feedback indicating whether their answers are correct or incorrect.

## 📁 Project Structure

```text
.
├── Course/
│   ├── 1 Databricks Lakehouse Architecture.md
│   ├── 2 Apache Spark Fundamentals.md
│   ├── 3 Delta Lake.md
│   ├── 4 Data Engineering with Spark SQL and PySpark.md
│   ├── 5 Structured Streaming.md
│   ├── 6 Databricks Workflows and Jobs.md
│   ├── 7 Unity Catalog and Governance.md
│   └── 8 Databricks SQL.md
│
├── Questions/
│   ├── 1.json
│   ├── 2.json
│   ├── 3.json
│   ├── 4.json
│   ├── 5.json
│   ├── 6.json
│   ├── 7.json
│   └── 8.json
│
├── quiz.html
└── README.md
```

## 📖 Course Content

Each Markdown file in the `Course/` directory covers a specific topic related to Databricks.

| No. | Course | Question File |
|---:|---|---|
| 1 | Databricks Lakehouse Architecture | `Questions/1.json` |
| 2 | Apache Spark Fundamentals | `Questions/2.json` |
| 3 | Delta Lake | `Questions/3.json` |
| 4 | Data Engineering with Spark SQL and PySpark | `Questions/4.json` |
| 5 | Structured Streaming | `Questions/5.json` |
| 6 | Databricks Workflows and Jobs | `Questions/6.json` |
| 7 | Unity Catalog and Governance | `Questions/7.json` |
| 8 | Databricks SQL | `Questions/8.json` |

Each course has its own question file, allowing users to review and practice individual topics independently.

## 🧠 How the Quiz System Works

The JSON files store the questions, answer choices, and correct answers. The `quiz.html` file provides an interactive interface for loading this data and delivering a practice quiz.

### Features

- **Question Loading:** Select a JSON file corresponding to the course you want to review.
- **Multiple-Choice Questions:** Display questions along with their available answer choices.
- **Timer:** Track the time spent during each practice session.
- **Immediate Feedback:** Display the result as soon as an answer is selected.
- **Visual Feedback:**
  - 🟢 Green indicates a correct answer.
  - 🔴 Red indicates an incorrect answer.
- **Topic-Based Practice:** Focus on one course at a time by loading its corresponding JSON file.

## 🚀 How to Use the Project

### 1. Review a Course

Open the Markdown file corresponding to the topic you want to study. For example:

`Course/1 Databricks Lakehouse Architecture.md`

Read the course material to understand the key concepts before attempting the questions.

### 2. Open the Quiz Interface

Open `quiz.html` in a web browser.

Depending on how the JSON files are loaded by the interface, you may need to select the question file manually or run the project using a local HTTP server.

### 3. Load a Quiz

Select the JSON file associated with the course you have studied.

For example, to review Apache Spark Fundamentals, load:

`Questions/2.json`

### 4. Answer the Questions

Read each question, select one of the available answers, and check the result using the visual feedback. The timer helps you practice under conditions similar to a timed certification exam.

### 5. Continue Learning

Repeat the process for each course, alternating between reading the course material, taking quizzes, and reviewing incorrect answers.

## 🎯 Learning Objective

The main objective of this project is to build a strong understanding of the Databricks ecosystem, reinforce essential data engineering concepts, and improve exam readiness through consistent practice.

By combining structured course materials with interactive quizzes, learners can identify knowledge gaps, track their progress, and strengthen their understanding of the topics relevant to their certification preparation.