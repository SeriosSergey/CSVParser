using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Diagnostics;
using System.IO;
using Npgsql;
using System.Threading;

namespace CSVParser
{
    class Program
    {
        private static int _dataCount = 1000000;

        static void Main(string[] args)
        {
            //Запускаем программу создания файла csv
            //CSVGeneratorLauncher();

            //Считываем данные из файла
            List<string[]> data = ReadFromCSV();
            Console.WriteLine("Данные считаны.");
            Console.ReadLine();
        }

        static void CSVGeneratorLauncher()
        {
            Process StartProcess = new Process(); // новый процесс
            StartProcess.StartInfo.FileName = AppDomain.CurrentDomain.BaseDirectory + "\\CSVGenerator.exe"; // путь к запускаемому файлу
            StartProcess.StartInfo.Arguments = $"\"data\" \"{_dataCount}\""; // эта строка указывается, если программа запускается с параметрами (здесь указан пример, для наглядности)
            StartProcess.Start(); // запускаем программу
            StartProcess.WaitForExit(600000);//Ждем до 10 минут
        }

        static List<string[]> ReadFromCSV()
        {
            List<string[]> data = new List<string[]>();
            string text = File.ReadAllText(AppDomain.CurrentDomain.BaseDirectory + "data.csv");
            List<string> ListStrings = text.Split(new[] { Environment.NewLine }, StringSplitOptions.None).ToList();
            foreach (string str in ListStrings)
            {
                data.Add(str.Split(';'));
            }
            return data;
        }

        static void SaveDataToPostgreSQL(List<string[]> data, int threadsCount)
        {
            DataBase dataBase = new DataBase("localhost", "postgres", "trewq123", "users");
            List<Thread> listThreads = new List<Thread>();
            //Создаем i потоков
            for (int i = 0; i < threadsCount; i++)
            {
                Thread thread = new Thread(()=>SaveObjects(dataBase, threadsCount, i,data));
                thread.Start();
                listThreads.Add(thread);
            }
            //Ожидаем завершения всех потоков
            while (listThreads.Any(t => t.IsAlive)){ }
        }

        static void SaveObjects(DataBase dataBase, int threadsCount,int treadNumber,List<string[]> data)
        {
            foreach (string[] strings in data.Where(s=>Convert.ToInt32(s[0])% threadsCount== treadNumber ))
            {
                dataBase.WriteData($"INSERT INTO users (id, fullname, email, phone) VALUES ('{strings[0]}','{strings[1]}','{strings[2]}','{strings[3]}')");
            }
        }

        class DataBase
        {
            readonly private string CS;

            public DataBase(string host, string username, string password, string database)
            {
                CS = $"Host = {host}; Port=5432; Username = {username}; Password = {password}; Database = {database}";
            }

            public NpgsqlDataReader ReadData(string запрос)
            {
                try
                {
                    NpgsqlConnection con = new NpgsqlConnection(CS);
                    con.Open();

                    var cmd = new NpgsqlCommand(запрос, con);

                    NpgsqlDataReader rdr = cmd.ExecuteReader();
                    return rdr;
                }
                catch
                {
                    throw;
                }
            }

            public void WriteData(string запрос)
            {
                try
                {
                    NpgsqlConnection con = new NpgsqlConnection(CS);
                    con.Open();
                    var cmd = new NpgsqlCommand(запрос, con);
                    cmd.ExecuteNonQuery();
                    con.Close();
                }
                catch
                {
                    throw;
                }
            }
        }
    }
}
