using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.IO;

namespace DataScript
{
    public class DataTable
    {
        public string Name { get; }
        public List<DataRow> Rows { get; } = new List<DataRow>();
        public Dictionary<string, DataColumn> Columns { get; } =
            new Dictionary<string, DataColumn>();

        public DataTable(string name)
        {
            Name = name;
        }

        public DataRow NewRow()
        {
            return new DataRow(this);
        }

        public void AddColumn(string name, Type dataType, object defaultValue = null)
        {
            Columns[name] = new DataColumn(name, dataType, defaultValue);

            foreach (var row in Rows)
            {
                row.Values[name] = defaultValue;
            }
        }

        public DataTable Filter(Func<DataRow, bool> predicate)
        {
            var result = new DataTable($"{Name}_filtered");

            foreach (var col in Columns.Values)
            {
                result.AddColumn(col.Name, col.DataType, col.DefaultValue);
            }

            foreach (var row in Rows.Where(predicate))
            {
                var newRow = result.NewRow();
                foreach (var col in Columns.Keys)
                {
                    newRow[col] = row[col];
                }
                result.Rows.Add(newRow);
            }

            return result;
        }

        public DataTable Select(params string[] columnNames)
        {
            var result = new DataTable($"{Name}_projection");

            foreach (var colName in columnNames)
            {
                if (Columns.TryGetValue(colName, out var column))
                {
                    result.AddColumn(column.Name, column.DataType, column.DefaultValue);
                }
            }

            foreach (var row in Rows)
            {
                var newRow = result.NewRow();
                foreach (var colName in columnNames)
                {
                    if (row.Values.TryGetValue(colName, out var value))
                    {
                        newRow[colName] = value;
                    }
                }
                result.Rows.Add(newRow);
            }

            return result;
        }

        public override string ToString()
        {
            var columnsList = string.Join(", ", Columns.Keys);
            return $"table {Name} with columns: {columnsList} ({Rows.Count} rows)";
        }

        public void Print(int maxRows = 10)
        {
            Console.WriteLine($"table: {Name} ({Rows.Count} rows)");
            Console.WriteLine(new string('-', 80));

            Console.WriteLine(string.Join(" | ", Columns.Keys.Select(k => k.PadRight(15))));
            Console.WriteLine(new string('-', 80));

            foreach (var row in Rows.Take(maxRows))
            {
                Console.WriteLine(
                    string.Join(
                        " | ",
                        Columns.Keys.Select(
                            k =>
                                row.Values.TryGetValue(k, out var v)
                                    ? v?.ToString()?.PadRight(15) ?? "NULL".PadRight(15)
                                    : "NULL".PadRight(15)
                        )
                    )
                );
            }

            if (Rows.Count > maxRows)
            {
                Console.WriteLine($"... and {Rows.Count - maxRows} more rows");
            }
        }
    }

    public class DataRow
    {
        private readonly DataTable _table;
        public Dictionary<string, object> Values { get; } = new Dictionary<string, object>();

        public DataRow(DataTable table)
        {
            _table = table;

            foreach (var col in table.Columns.Values)
            {
                Values[col.Name] = col.DefaultValue;
            }
        }

        public object this[string columnName]
        {
            get => Values.TryGetValue(columnName, out var value) ? value : null;
            set => Values[columnName] = value;
        }

        public override string ToString()
        {
            return $"row in {_table.Name}: {string.Join(", ", Values.Select(kv => $" {kv.Key}= {kv.Value}"))}";
        }
    }

    public class DataColumn
    {
        public string Name { get; }
        public Type DataType { get; }
        public object DefaultValue { get; }

        public DataColumn(string name, Type dataType, object defaultValue = null)
        {
            Name = name;
            DataType = dataType;
            DefaultValue = defaultValue;
        }
    }

    public class DataSet
    {
        private readonly Dictionary<string, DataTable> _tables = new Dictionary<
            string,
            DataTable
        >();

        public DataTable this[string tableName]
        {
            get => _tables.TryGetValue(tableName, out var table) ? table : null;
        }

        public DataTable CreateTable(string name)
        {
            var table = new DataTable(name);
            _tables[name] = table;
            return table;
        }

        public void RemoveTable(string name)
        {
            _tables.Remove(name);
        }

        public bool TableExists(string name)
        {
            return _tables.ContainsKey(name);
        }

        public IEnumerable<DataTable> Tables => _tables.Values;

        public void LoadFromJson(string json)
        {
            try
            {
                var jsonDoc = JsonDocument.Parse(json);

                foreach (var tableProp in jsonDoc.RootElement.EnumerateObject())
                {
                    var tableName = tableProp.Name;
                    var table = CreateTable(tableName);

                    bool isFirst = true;

                    foreach (var rowElement in tableProp.Value.EnumerateArray())
                    {
                        if (isFirst)
                        {
                            foreach (var prop in rowElement.EnumerateObject())
                            {
                                Type dataType;
                                switch (prop.Value.ValueKind)
                                {
                                    case JsonValueKind.Number:
                                        if (prop.Value.TryGetInt32(out _))
                                            dataType = typeof(int);
                                        else
                                            dataType = typeof(decimal);
                                        break;
                                    case JsonValueKind.String:
                                        dataType = typeof(string);
                                        break;
                                    case JsonValueKind.True:
                                    case JsonValueKind.False:
                                        dataType = typeof(bool);
                                        break;
                                    case JsonValueKind.Null:

                                        dataType = typeof(string);
                                        break;
                                    default:
                                        dataType = typeof(string);
                                        break;
                                }
                                table.AddColumn(prop.Name, dataType);
                            }
                            isFirst = false;
                        }

                        var row = table.NewRow();
                        foreach (var prop in rowElement.EnumerateObject())
                        {
                            if (!table.Columns.TryGetValue(prop.Name, out var column))
                                continue;

                            if (prop.Value.ValueKind == JsonValueKind.Null)
                            {
                                row[prop.Name] = null;
                                continue;
                            }

                            try
                            {
                                switch (column.DataType)
                                {
                                    case Type t when t == typeof(int):
                                        row[prop.Name] = prop.Value.GetInt32();
                                        break;
                                    case Type t when t == typeof(decimal):
                                        row[prop.Name] = prop.Value.GetDecimal();
                                        break;
                                    case Type t when t == typeof(bool):
                                        row[prop.Name] = prop.Value.GetBoolean();
                                        break;
                                    default:
                                        row[prop.Name] = prop.Value.GetString();
                                        break;
                                }
                            }
                            catch
                            {
                                row[prop.Name] = prop.Value.ToString();
                            }
                        }
                        table.Rows.Add(row);
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"error loading JSON: {ex.Message}");
                throw;
            }
        }

        public string SaveToJson()
        {
            var result = new Dictionary<string, List<Dictionary<string, object>>>();

            foreach (var table in _tables.Values)
            {
                var rows = new List<Dictionary<string, object>>();
                foreach (var row in table.Rows)
                {
                    rows.Add(row.Values);
                }
                result[table.Name] = rows;
            }

            return JsonSerializer.Serialize(
                result,
                new JsonSerializerOptions { WriteIndented = true }
            );
        }
    }

    public class DataScriptInterpreter
    {
        private DataSet _dataSet = new DataSet();
        private Dictionary<string, object> _variables = new Dictionary<string, object>();

        private readonly Dictionary<string, Func<string[], object>> _commands = new Dictionary<
            string,
            Func<string[], object>
        >();

        public DataScriptInterpreter()
        {
            InitializeCommands();
        }

        private void InitializeCommands()
        {
            _commands["create"] = CreateTable;
            _commands["load"] = LoadData;
            _commands["save"] = SaveData;
            _commands["add"] = AddData;
            _commands["filter"] = FilterData;
            _commands["select"] = SelectColumns;
            _commands["show"] = ShowTable;
            _commands["list"] = ListTables;
            _commands["import"] = ImportCsv;
            _commands["export"] = ExportCsv;
            _commands["join"] = JoinTables;
            _commands["sort"] = SortTable;
            _commands["count"] = CountRows;
            _commands["sum"] = SumColumn;
            _commands["avg"] = AverageColumn;
            _commands["min"] = MinColumn;
            _commands["max"] = MaxColumn;

            _commands["show"] = args =>
            {
                if (args.Length < 1)
                    throw new ArgumentException(
                        "usage: show \"text\" or show 'text' or show \"tableName\""
                    );

                var text = args[0];

                if (_dataSet.TableExists(text))
                {
                    return ShowTable(args);
                }

                var isDoubleQuoted = text.StartsWith("\"") && text.EndsWith("\"");
                var isSingleQuoted = text.StartsWith("'") && text.EndsWith("'");

                if (!isDoubleQuoted && !isSingleQuoted)
                {
                    throw new ArgumentException(
                        "text values must be quoted. Use: show \"text\" or show 'text'"
                    );
                }

                var unquoted = text.Substring(1, text.Length - 2);
                var fullText =
                    args.Length > 1 ? unquoted + " " + string.Join(" ", args.Skip(1)) : unquoted;

                Console.WriteLine(fullText);
                return null;
            };
        }

        public void ExecuteFile(string filePath)
        {
            try
            {
                if (!File.Exists(filePath))
                {
                    Console.WriteLine($"error: File not found: {filePath}");
                    return;
                }

                string scriptText = File.ReadAllText(filePath);
                Execute(scriptText);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"error executing: {ex.Message}");
            }
        }

        public object Execute(string scriptText)
        {
            var lines = scriptText.Split(
                new[] { '\r', '\n' },
                StringSplitOptions.RemoveEmptyEntries
            );
            object lastResult = null;

            foreach (var line in lines)
            {
                var trimmedLine = line.Trim();
                if (string.IsNullOrEmpty(trimmedLine) || trimmedLine.StartsWith("//"))
                    continue;

                try
                {
                    lastResult = ExecuteLine(trimmedLine);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"error: {ex.Message} in line: {trimmedLine}");
                }
            }

            return lastResult;
        }

        public object ExecuteLine(string line)
        {
            var parts = SplitCommand(line);
            if (parts.Length == 0)
                return null;

            var command = parts[0].ToLower();
            var args = parts.Skip(1).ToArray();

            if (_commands.TryGetValue(command, out var handler))
            {
                var result = handler(args);

                if (result != null && !(result is DataRow))
                {
                    if (result is DataTable table)
                    {
                        table.Print();
                    }
                    else if (
                        !(result is string stringResult && stringResult.StartsWith("Row added"))
                    )
                    {
                        Console.WriteLine(result);
                    }
                }
                return result;
            }

            throw new ArgumentException($"unknown command: {command}");
        }

        private string[] SplitCommand(string command)
        {
            var parts = new List<string>();
            var inSingleQuotes = false;
            var inDoubleQuotes = false;
            var current = "";

            for (int i = 0; i < command.Length; i++)
            {
                var c = command[i];

                if (c == '"' && !inSingleQuotes)
                {
                    inDoubleQuotes = !inDoubleQuotes;
                    current += c;
                    continue;
                }
                if (c == '\'' && !inDoubleQuotes)
                {
                    inSingleQuotes = !inSingleQuotes;
                    current += c;
                    continue;
                }

                if (c == ' ' && !inSingleQuotes && !inDoubleQuotes)
                {
                    if (!string.IsNullOrEmpty(current))
                    {
                        parts.Add(current);
                        current = "";
                    }
                }
                else
                {
                    current += c;
                }
            }

            if (inSingleQuotes || inDoubleQuotes)
            {
                throw new ArgumentException(
                    "Unclosed quotes in command. Make sure to close all quotes."
                );
            }

            if (!string.IsNullOrEmpty(current))
            {
                parts.Add(current);
            }

            return parts.ToArray();
        }
        private object CreateTable(string[] args)
        {
            if (args.Length < 2)
                throw new ArgumentException(
                    "usage: create [tableName] [column1:type] [column2:type] ..."
                );

            var tableName = args[0];

            if (_dataSet.TableExists(tableName))
            {
                _dataSet.RemoveTable(tableName);
            }

            var table = _dataSet.CreateTable(tableName);

            for (int i = 1; i < args.Length; i++)
            {
                var colParts = args[i].Split(':');
                if (colParts.Length != 2)
                    throw new ArgumentException($"invalid column definition: {args[i]}");

                var colName = colParts[0];
                var dataType = colParts[1].ToLower() switch
                {
                    "int" => typeof(int),
                    "string" => typeof(string),
                    "decimal" => typeof(decimal),
                    "bool" => typeof(bool),
                    _ => throw new ArgumentException($"unknown data type: {colParts[1]}")
                };

                table.AddColumn(colName, dataType);
            }

            return table;
        }

        private object LoadData(string[] args)
        {
            if (args.Length < 1)
                throw new ArgumentException("usage: load [fileName]");

            var fileName = args[0];
            try
            {
                var json = File.ReadAllText(fileName);
                _dataSet.LoadFromJson(json);
                return $"loaded data from {fileName}";
            }
            catch (Exception ex)
            {
                throw new Exception($"failed to load file {fileName}: {ex.Message}");
            }
        }

        private object SaveData(string[] args)
        {
            if (args.Length < 1)
                throw new ArgumentException("usage: save [fileName]");

            var fileName = args[0];
            try
            {
                var json = _dataSet.SaveToJson();
                File.WriteAllText(fileName, json);
                return $"saved data to {fileName}";
            }
            catch (Exception ex)
            {
                throw new Exception($"failed to save file {fileName}: {ex.Message}");
            }
        }

        private object AddData(string[] args)
        {
            if (args.Length < 2)
                throw new ArgumentException(
                    "usage: add [tableName] [col1=\"value1\"] or [col1='value1'] [col2=value2] ..."
                );

            var tableName = args[0];
            var table = _dataSet[tableName];
            if (table == null)
                throw new ArgumentException($"table not found: {tableName}");

            var row = table.NewRow();

            for (int i = 1; i < args.Length; i++)
            {
                var assignParts = args[i].Split(new[] { '=' }, 2);
                if (assignParts.Length != 2)
                    throw new ArgumentException($"invalid assignment: {args[i]}");

                var colName = assignParts[0];
                var valueText = assignParts[1];

                if (!table.Columns.TryGetValue(colName, out var column))
                    throw new ArgumentException($"column not found: {colName}");

                object value;
                try
                {
                    if (valueText.ToLower() == "null")
                        value = null;
                    else if (column.DataType == typeof(string))
                    {
                        var isDoubleQuoted = valueText.StartsWith("\"") && valueText.EndsWith("\"");
                        var isSingleQuoted = valueText.StartsWith("'") && valueText.EndsWith("'");

                        if (!isDoubleQuoted && !isSingleQuoted)
                        {
                            throw new ArgumentException(
                                $"String values must be quoted. Use: {colName}=\"{valueText}\" or {colName}='{valueText}'"
                            );
                        }

                        value = valueText.Substring(1, valueText.Length - 2);
                    }
                    else
                        value = Convert.ChangeType(valueText, column.DataType);
                }
                catch (ArgumentException)
                {
                    throw;
                }
                catch
                {
                    throw new ArgumentException(
                        $"cannot convert value '{valueText}' to {column.DataType.Name}"
                    );
                }

                row[colName] = value;
            }

            table.Rows.Add(row);
            return $"row added to {tableName}";
        }

        private object FilterData(string[] args)
        {
            if (args.Length < 2)
                throw new ArgumentException(
                    "usage: filter [tableName] [conditions]\n"
                        + "conditions: [column] [operator] [value] [AND|OR] ...\n"
                        + "operators: =, !=, >, >=, <, <=, contains, startswith, endswith, isnull, notnull, in, between, like, matches"
                );

            var tableName = args[0];
            var table = _dataSet[tableName];
            if (table == null)
                throw new ArgumentException($"table not found: {tableName}");

            var conditions = new List<(string column, string op, object value, string logic)>();
            string currentLogic = "AND";

            for (int i = 1; i < args.Length; i++)
            {
                if (i + 2 >= args.Length)
                    break;

                var columnName = args[i];
                var op = args[i + 1].ToLower();
                var valueText = args[i + 2];

                if (!table.Columns.TryGetValue(columnName, out var column))
                    throw new ArgumentException($"column not found: {columnName}");

                object value = null;
                if (op == "between" && i + 3 < args.Length)
                {
                    var value1 = ParseValue(valueText, column.DataType);
                    var value2 = ParseValue(args[i + 3], column.DataType);
                    value = (value1, value2);
                    i++;
                }
                else if (op == "in")
                {
                    var values = valueText
                        .Split(',')
                        .Select(v => ParseValue(v.Trim(), column.DataType))
                        .ToList();
                    value = values;
                }
                else if (op != "isnull" && op != "notnull")
                {
                    value = ParseValue(valueText, column.DataType);
                }

                conditions.Add((columnName, op, value, currentLogic));

                if (
                    i + 3 < args.Length
                    && (args[i + 3].ToUpper() == "AND" || args[i + 3].ToUpper() == "OR")
                )
                {
                    currentLogic = args[i + 3].ToUpper();
                    i++;
                }

                i += 2;
            }

            Func<DataRow, bool> predicate = row =>
            {
                bool result = true;
                string lastLogic = "AND";

                foreach (var (columnName, op, value, logic) in conditions)
                {
                    bool conditionResult = EvaluateCondition(row[columnName], op, value);

                    if (lastLogic == "AND")
                        result = result && conditionResult;
                    else
                        result = result || conditionResult;

                    lastLogic = logic;
                }

                return result;
            };

            return table.Filter(predicate);
        }

        private object ParseValue(string valueText, Type targetType)
        {
            if (string.IsNullOrEmpty(valueText) || valueText.ToLower() == "null")
                return null;

            try
            {
                if (targetType == typeof(string))
                    return valueText;
                else if (targetType == typeof(DateTime))
                    return DateTime.Parse(valueText);
                else
                    return Convert.ChangeType(valueText, targetType);
            }
            catch
            {
                throw new ArgumentException(
                    $"cannot convert value '{valueText}' to {targetType.Name}"
                );
            }
        }

        private bool EvaluateCondition(object rowValue, string op, object filterValue)
        {
            if (op == "isnull")
                return rowValue == null;

            if (op == "notnull")
                return rowValue != null;

            if (rowValue == null)
                return false;

            switch (op)
            {
                case "=":
                    return ObjectEquals(rowValue, filterValue);
                case "!=":
                    return !ObjectEquals(rowValue, filterValue);
                case ">":
                    return CompareValues(rowValue, filterValue) > 0;
                case ">=":
                    return CompareValues(rowValue, filterValue) >= 0;
                case "<":
                    return CompareValues(rowValue, filterValue) < 0;
                case "<=":
                    return CompareValues(rowValue, filterValue) <= 0;
                case "contains":
                    return rowValue
                        .ToString()
                        .Contains(
                            filterValue?.ToString() ?? "",
                            StringComparison.OrdinalIgnoreCase
                        );
                case "startswith":
                    return rowValue
                        .ToString()
                        .StartsWith(
                            filterValue?.ToString() ?? "",
                            StringComparison.OrdinalIgnoreCase
                        );
                case "endswith":
                    return rowValue
                        .ToString()
                        .EndsWith(
                            filterValue?.ToString() ?? "",
                            StringComparison.OrdinalIgnoreCase
                        );
                case "like":
                    return MatchLikePattern(rowValue.ToString(), filterValue?.ToString() ?? "");
                case "matches":
                    return System.Text.RegularExpressions.Regex.IsMatch(
                        rowValue.ToString(),
                        filterValue?.ToString() ?? ""
                    );
                case "in":
                    return filterValue is List<object> list
                        && list.Any(v => ObjectEquals(rowValue, v));
                case "between":
                    if (filterValue is ValueTuple<object, object> range)
                    {
                        var (min, max) = range;
                        return CompareValues(rowValue, min) >= 0
                            && CompareValues(rowValue, max) <= 0;
                    }
                    return false;
                default:
                    throw new ArgumentException($"unknown operator: {op}");
            }
        }

        private bool MatchLikePattern(string value, string pattern)
        {
            pattern = System.Text.RegularExpressions.Regex
                .Escape(pattern)
                .Replace("%", ".*")
                .Replace("_", ".");
            return System.Text.RegularExpressions.Regex.IsMatch(
                value,
                $"^{pattern}$",
                System.Text.RegularExpressions.RegexOptions.IgnoreCase
            );
        }

        private bool ObjectEquals(object a, object b)
        {
            if (a == null && b == null)
                return true;
            if (a == null || b == null)
                return false;

            try
            {
                var convertedB = Convert.ChangeType(b, a.GetType());
                return a.Equals(convertedB);
            }
            catch
            {
                return a.Equals(b);
            }
        }

        private int CompareValues(object a, object b)
        {
            if (a == null && b == null)
                return 0;
            if (a == null)
                return -1;
            if (b == null)
                return 1;

            if (a is IComparable ca)
            {
                try
                {
                    var convertedB = Convert.ChangeType(b, a.GetType());
                    return ca.CompareTo(convertedB);
                }
                catch
                {
                    return ca.CompareTo(b);
                }
            }

            return a.ToString().CompareTo(b.ToString());
        }

        private object SelectColumns(string[] args)
        {
            if (args.Length < 2)
                throw new ArgumentException("usage: select [tableName] [column1] [column2] ...");

            var tableName = args[0];
            var table = _dataSet[tableName];
            if (table == null)
                throw new ArgumentException($"table not found: {tableName}");

            var columns = args.Skip(1).ToArray();
            return table.Select(columns);
        }

        private object ShowTable(string[] args)
        {
            if (args.Length < 1)
                throw new ArgumentException("usage: show [tableName]");

            var tableName = args[0];
            var table = _dataSet[tableName];
            if (table == null)
                throw new ArgumentException($"table not found: {tableName}");

            int maxRows = 20;
            if (args.Length > 1 && int.TryParse(args[1], out var parsedMaxRows))
            {
                maxRows = parsedMaxRows;
            }

            table.Print(maxRows);
            return null;
        }

        private object ListTables(string[] args)
        {
            Console.WriteLine("available tables:");
            foreach (var table in _dataSet.Tables)
            {
                Console.WriteLine($"- {table.Name} ({table.Rows.Count} rows)");
            }
            return null;
        }

        private object ImportCsv(string[] args)
        {
            if (args.Length < 2)
                throw new ArgumentException("usage: import [fileName] [tableName]");

            var fileName = args[0];
            var tableName = args[1];

            try
            {
                var lines = File.ReadAllLines(fileName);
                if (lines.Length == 0)
                    throw new ArgumentException("CSV file is empty");

                var headers = ParseCsvLine(lines[0]);
                var table = _dataSet.CreateTable(tableName);

                foreach (var header in headers)
                {
                    table.AddColumn(header.Trim(), typeof(string));
                }

                for (int i = 1; i < lines.Length; i++)
                {
                    if (string.IsNullOrWhiteSpace(lines[i]))
                        continue;

                    var values = ParseCsvLine(lines[i]);
                    var row = table.NewRow();

                    for (int j = 0; j < Math.Min(headers.Length, values.Length); j++)
                    {
                        row[headers[j].Trim()] = values[j];
                    }

                    table.Rows.Add(row);
                }

                InferColumnTypes(table);

                return table;
            }
            catch (Exception ex)
            {
                throw new Exception($"error importing CSV: {ex.Message}");
            }
        }

        private void InferColumnTypes(DataTable table)
        {
            foreach (var column in table.Columns.Values.ToList())
            {
                bool canBeInt = true;
                bool canBeDecimal = true;
                bool canBeBool = true;

                foreach (var row in table.Rows)
                {
                    var value = row[column.Name]?.ToString();
                    if (string.IsNullOrEmpty(value))
                        continue;

                    if (canBeInt && !int.TryParse(value, out _))
                        canBeInt = false;

                    if (canBeDecimal && !decimal.TryParse(value, out _))
                        canBeDecimal = false;

                    if (canBeBool && !bool.TryParse(value, out _))
                        canBeBool = false;

                    if (!canBeInt && !canBeDecimal && !canBeBool)
                        break;
                }

                Type newType;
                if (canBeInt)
                    newType = typeof(int);
                else if (canBeDecimal)
                    newType = typeof(decimal);
                else if (canBeBool)
                    newType = typeof(bool);
                else
                    continue;

                table.AddColumn(column.Name + "_temp", newType);

                foreach (var row in table.Rows)
                {
                    var value = row[column.Name]?.ToString();
                    if (!string.IsNullOrEmpty(value))
                    {
                        try
                        {
                            row[column.Name + "_temp"] = Convert.ChangeType(value, newType);
                        }
                        catch
                        {
                            row[column.Name + "_temp"] = null;
                        }
                    }
                }

                var tempColName = column.Name + "_temp";
                var newCol = table.Columns[tempColName];
                table.Columns.Remove(column.Name);
                table.AddColumn(column.Name, newType);

                foreach (var row in table.Rows)
                {
                    row[column.Name] = row[tempColName];
                    row.Values.Remove(tempColName);
                }

                table.Columns.Remove(tempColName);
            }
        }

        private string[] ParseCsvLine(string line)
        {
            var result = new List<string>();
            var inQuotes = false;
            var current = "";

            for (int i = 0; i < line.Length; i++)
            {
                var c = line[i];

                if (c == '"')
                {
                    if (inQuotes && i + 1 < line.Length && line[i + 1] == '"')
                    {
                        current += '"';
                        i++;
                    }
                    else
                    {
                        inQuotes = !inQuotes;
                    }
                }
                else if (c == ',' && !inQuotes)
                {
                    result.Add(current);
                    current = "";
                }
                else
                {
                    current += c;
                }
            }

            result.Add(current);
            return result.ToArray();
        }

        private object ExportCsv(string[] args)
        {
            if (args.Length < 2)
                throw new ArgumentException("usage: export [tableName] [fileName]");

            var tableName = args[0];
            var fileName = args[1];

            var table = _dataSet[tableName];
            if (table == null)
                throw new ArgumentException($"table not found: {tableName}");

            var csvLines = new List<string>();

            csvLines.Add(string.Join(",", table.Columns.Keys));

            foreach (var row in table.Rows)
            {
                var values = table.Columns.Keys
                    .Select(k => row[k]?.ToString() ?? "")
                    .Select(EscapeCsvValue)
                    .ToArray();

                csvLines.Add(string.Join(",", values));
            }

            File.WriteAllLines(fileName, csvLines);
            return $"exported {table.Rows.Count} rows to {fileName}";
        }

        private string EscapeCsvValue(string value)
        {
            if (value.Contains(",") || value.Contains("\"") || value.Contains("\n"))
            {
                return $"\"{value.Replace("\"", "\"\"")}\"";
            }
            return value;
        }

        private object JoinTables(string[] args)
        {
            if (args.Length < 5)
                throw new ArgumentException(
                    "usage: join [leftTable] [rightTable] [leftColumn] [rightColumn] [resultTable]"
                );

            var leftTableName = args[0];
            var rightTableName = args[1];
            var leftColumnName = args[2];
            var rightColumnName = args[3];
            var resultTableName = args[4];

            var leftTable = _dataSet[leftTableName];
            var rightTable = _dataSet[rightTableName];

            if (leftTable == null)
                throw new ArgumentException($"table not found: {leftTableName}");
            if (rightTable == null)
                throw new ArgumentException($"table not found: {rightTableName}");

            if (!leftTable.Columns.ContainsKey(leftColumnName))
                throw new ArgumentException(
                    $"column not found in {leftTableName}: {leftColumnName}"
                );
            if (!rightTable.Columns.ContainsKey(rightColumnName))
                throw new ArgumentException(
                    $"column not found in {rightTableName}: {rightColumnName}"
                );

            var resultTable = _dataSet.CreateTable(resultTableName);

            foreach (var col in leftTable.Columns.Values)
            {
                resultTable.AddColumn(
                    leftTableName + "_" + col.Name,
                    col.DataType,
                    col.DefaultValue
                );
            }

            foreach (var col in rightTable.Columns.Values)
            {
                resultTable.AddColumn(
                    rightTableName + "_" + col.Name,
                    col.DataType,
                    col.DefaultValue
                );
            }

            foreach (var leftRow in leftTable.Rows)
            {
                var leftValue = leftRow[leftColumnName];

                foreach (var rightRow in rightTable.Rows)
                {
                    var rightValue = rightRow[rightColumnName];

                    if (ObjectEquals(leftValue, rightValue))
                    {
                        var resultRow = resultTable.NewRow();

                        foreach (var col in leftTable.Columns.Keys)
                        {
                            resultRow[leftTableName + "_" + col] = leftRow[col];
                        }

                        foreach (var col in rightTable.Columns.Keys)
                        {
                            resultRow[rightTableName + "_" + col] = rightRow[col];
                        }

                        resultTable.Rows.Add(resultRow);
                    }
                }
            }

            return resultTable;
        }

        private object SortTable(string[] args)
        {
            if (args.Length < 2)
                throw new ArgumentException("usage: sort [tableName] [columnName] [asc|desc]");

            var tableName = args[0];
            var columnName = args[1];
            var direction = args.Length > 2 ? args[2].ToLower() : "asc";

            var table = _dataSet[tableName];
            if (table == null)
                throw new ArgumentException($"table not found: {tableName}");

            if (!table.Columns.ContainsKey(columnName))
                throw new ArgumentException($"column not found: {columnName}");

            var isAscending = direction != "desc";

            var resultTable = new DataTable($"{tableName}_sorted");

            foreach (var col in table.Columns.Values)
            {
                resultTable.AddColumn(col.Name, col.DataType, col.DefaultValue);
            }

            var sortedRows = isAscending
                ? table.Rows.OrderBy(
                      r => r[columnName],
                      Comparer<object>.Create((a, b) => CompareValues(a, b))
                  )
                : table.Rows.OrderByDescending(
                      r => r[columnName],
                      Comparer<object>.Create((a, b) => CompareValues(a, b))
                  );

            foreach (var row in sortedRows)
            {
                var newRow = resultTable.NewRow();
                foreach (var col in table.Columns.Keys)
                {
                    newRow[col] = row[col];
                }
                resultTable.Rows.Add(newRow);
            }

            return resultTable;
        }

        private object CountRows(string[] args)
        {
            if (args.Length < 1)
                throw new ArgumentException("usage: count [tableName]");

            var tableName = args[0];
            var table = _dataSet[tableName];
            if (table == null)
                throw new ArgumentException($"table not found: {tableName}");

            return $"count: {table.Rows.Count}";
        }

        private object SumColumn(string[] args)
        {
            if (args.Length < 2)
                throw new ArgumentException("usage: sum [tableName] [columnName]");

            var tableName = args[0];
            var columnName = args[1];

            var table = _dataSet[tableName];
            if (table == null)
                throw new ArgumentException($"table not found: {tableName}");

            if (!table.Columns.ContainsKey(columnName))
                throw new ArgumentException($"column not found: {columnName}");

            var column = table.Columns[columnName];
            if (column.DataType != typeof(int) && column.DataType != typeof(decimal))
                throw new ArgumentException($"column {columnName} is not numeric");

            decimal sum = 0;
            foreach (var row in table.Rows)
            {
                if (row[columnName] != null)
                {
                    sum += Convert.ToDecimal(row[columnName]);
                }
            }

            return $"sum of {columnName}: {sum}";
        }

        private object AverageColumn(string[] args)
        {
            if (args.Length < 2)
                throw new ArgumentException("usage: avg [tableName] [columnName]");

            var tableName = args[0];
            var columnName = args[1];

            var table = _dataSet[tableName];
            if (table == null)
                throw new ArgumentException($"table not found: {tableName}");

            if (!table.Columns.ContainsKey(columnName))
                throw new ArgumentException($"column not found: {columnName}");

            var column = table.Columns[columnName];
            if (column.DataType != typeof(int) && column.DataType != typeof(decimal))
                throw new ArgumentException($"column {columnName} is not numeric");

            decimal sum = 0;
            int count = 0;
            foreach (var row in table.Rows)
            {
                if (row[columnName] != null)
                {
                    sum += Convert.ToDecimal(row[columnName]);
                    count++;
                }
            }

            if (count == 0)
                return $"average of {columnName}: 0 (no data)";

            return $"average of {columnName}: {sum / count}";
        }

        private object MinColumn(string[] args)
        {
            if (args.Length < 2)
                throw new ArgumentException("usage: min [tableName] [columnName]");

            var tableName = args[0];
            var columnName = args[1];

            var table = _dataSet[tableName];
            if (table == null)
                throw new ArgumentException($"table not found: {tableName}");

            if (!table.Columns.ContainsKey(columnName))
                throw new ArgumentException($"column not found: {columnName}");

            var values = table.Rows.Select(r => r[columnName]).Where(v => v != null).ToList();

            if (values.Count == 0)
                return $"min of {columnName}: undefined (no data)";

            var min = values.Min();
            return $"min of {columnName}: {min}";
        }

        private object MaxColumn(string[] args)
        {
            if (args.Length < 2)
                throw new ArgumentException("usage: max [tableName] [columnName]");

            var tableName = args[0];
            var columnName = args[1];

            var table = _dataSet[tableName];
            if (table == null)
                throw new ArgumentException($"table not found: {tableName}");

            if (!table.Columns.ContainsKey(columnName))
                throw new ArgumentException($"column not found: {columnName}");

            var values = table.Rows.Select(r => r[columnName]).Where(v => v != null).ToList();

            if (values.Count == 0)
                return $"max of {columnName}: undefined (no data)";

            var max = values.Max();
            return $"max of {columnName}: {max}";
        }
    }

    public static class Program
    {
        public static void Main(string[] args)
        {
            var interpreter = new DataScriptInterpreter();

            if (args.Length > 0)
            {
                string scriptPath = args[0];
                if (
                    Path.GetExtension(scriptPath) == ".dscript"
                    || Path.GetExtension(scriptPath) == ".txt"
                )
                {
                    interpreter.ExecuteFile(scriptPath);
                }
                else
                {
                    Console.WriteLine(
                        $"unsupported file extension: {Path.GetExtension(scriptPath)}"
                    );
                    Console.WriteLine("supported extensions: .dscript, .txt");
                }
            }
            else
            {
                var script =
                    @"
						show 'hello world!'
			     ";

                interpreter.Execute(script);
            }
        }
    }
}
