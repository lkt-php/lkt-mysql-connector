<?php

namespace Lkt\DatabaseConnectors;

use Lkt\DatabaseConnectors\Cache\QueryCache;
use Lkt\Factory\Schemas\Fields\AbstractField;
use Lkt\Factory\Schemas\Fields\JSONField;
use Lkt\Factory\Schemas\Fields\PivotField;
use Lkt\Factory\Schemas\Fields\RelatedField;
use Lkt\Factory\Schemas\Fields\RelatedKeysField;
use Lkt\Factory\Schemas\Schema;
use Lkt\Locale\Locale;

class MySQLConnector extends DatabaseConnector
{
    protected $port = 3306;
    protected $charset = 'utf8';
    protected $rememberTotal = '';

    /**
     * @param string $rememberTotal
     * @return $this
     */
    public function setRememberTotal(string $rememberTotal): MySQLConnector
    {
        $this->rememberTotal = $rememberTotal;
        return $this;
    }

    /**
     * @return DatabaseConnector
     */
    public function connect(): DatabaseConnector
    {
        if ($this->connection !== null) {
            return $this;
        }

        // Perform the connection
        try {
            $this->connection = new \PDO (
                "mysql:host={$this->host};dbname={$this->database};charset={$this->charset}",
                $this->user,
                $this->password
            );

        } catch (\Exception $e) {
            die ('Connection to database failed');
        }
        return $this;
    }

    /**
     * @return DatabaseConnector
     */
    public function disconnect(): DatabaseConnector
    {
        $this->connection = null;
        return $this;
    }

    /**
     * @param string $query
     * @param array $replacements
     * @return array|null
     */
    public function query(string $query, array $replacements = []):? array
    {
        $this->connect();
        $sql = ConnectionHelper::sanitizeQuery($query);
        $sql = ConnectionHelper::prepareParams($sql, $replacements);
        $sql = \str_replace('_LANG', '_' . Locale::getLangCode(), $sql);
        if ($this->rememberTotal !== '') {
            $sql = \preg_replace('/SELECT/i', 'SELECT SQL_CALC_FOUND_ROWS', $sql, 1);
            $sql .= '; SET @rows_' . $this->rememberTotal . ' = FOUND_ROWS();';
        }

        $sql = trim($sql);
        $isSelect = strpos(strtolower($sql), 'select') === 0;

        // check if cached (only select queries)
        if ($isSelect && !$this->forceRefresh && !$this->ignoreCache && QueryCache::isset($this->name, $sql)) {
            return QueryCache::get($this->name, $sql)->getLatestResults();
        }

        // fetch
        $result = $this->connection->query($sql, \PDO::FETCH_ASSOC);

        if ($this->forceRefresh) {
            $this->forceRefreshFinished();
        }

        if ($result === true || $result === false) {
            QueryCache::set($this->name, $sql, null);
            return null;
        }

        $r = [];
        foreach ($result as $row) {
            $r[] = $row;
        }

        QueryCache::set($this->name, $sql, $r);
        return $r;
    }

    /**
     * @param Schema $schema
     * @return array
     * @throws \Lkt\Factory\Schemas\Exceptions\InvalidComponentException
     */
    public function extractSchemaColumns(Schema $schema): array
    {
        $table = $schema->getTable();

        /** @var AbstractField[] $fields */
        $fields = $schema->getAllFields();

        $r = [];

        foreach ($fields as $key => $field) {
            if ($field instanceof PivotField || $field instanceof RelatedField || $field instanceof RelatedKeysField) {
                continue;
            }
            $column = trim($field->getColumn());
            if ($field instanceof JSONField && $field->isCompressed()) {
                $r[] = "UNCOMPRESS({$table}.{$column}) as {$key}";
            } else {
                $r[] = "{$table}.{$column} as {$key}";
            }
        }

        return $r;
    }

    /**
     * @return int
     */
    public function getLastInsertedId(): int
    {
        if ($this->connection === null) {
            return 0;
        }
        return (int)$this->connection->lastInsertId();
    }
}