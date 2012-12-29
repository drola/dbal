<?php

namespace Doctrine\DBAL\Driver\Ibase;

class IbaseConnection implements \Doctrine\DBAL\Driver\Connection {

    /**
     *
     * @var mixed Firebird/InterBase link identifier
     */
    private $_conn = null;

    /**
     *
     * @var mixed Firebird/Interbase transaction resource
     */
    private $_transResource = null;

    public function __construct(array $params, $username, $password, array $driverOptions = array()) {
        if (!extension_loaded('interbase')) {
            throw new IbaseException("The Interbase extension is required to connect to Interbase/Firebird database.");
        }

        $isPersistant = (isset($params['persistent']) && $params['persistent'] == true);

        if ($isPersistant) {
            $this->_conn = @ibase_pconnect(
                            $this->_formatDbConnString(
                                    $params['dbname'], $params['host'], $params['port']), $username, $password, $params['charset'], $params['buffers'], $params['dialect'], $params['role']
            );
        } else {
            $this->_conn = @ibase_connect(
                            $this->_formatDbConnString(
                                    $params['dbname'], $params['host'], $params['port']), $username, $password, $params['charset'], $params['buffers'], $params['dialect'], $params['role']
            );
        }

        if (!$this->_conn) {
            throw new IbaseException(ibase_errmsg());
        }
    }

    /**
     * Format a connection string
     * 
     * @param string $dbname Database name/path
     * @param type $host Host
     * @param string $port Port
     * @return string Connection string
     */
    protected function _formatDbConnString($dbname, $host, $port) {
        if (is_numeric($port)) {
            $port = '/' . (integer) $port;
        }
        if ($dbname) {
            $dbname = ':' . $dbname;
        }
        return $host . $port . $dbname;
    }

    public function beginTransaction() {
        $this->_transResource = ibase_trans(IBASE_DEFAULT, $this->_conn);
    }

    public function commit() {
        if (!ibase_commit($this->_transResource ? $this->_transResource : $this->_conn)) {
            throw new IbaseException(ibase_errmsg());
        }
    }

    public function errorCode() {
        return ibase_errcode();
    }

    public function errorInfo() {
        return ibase_errmsg();
    }

    public function exec($statement) {
        $stmt = $this->prepare($statement);
        $stmt->execute();
        return $stmt->rowCount();
    }

    public function lastInsertId($name = null, $primaryKey = null) {
        if ($name != null) {
            $sequence = $name;
            if ($primaryKey) {
                $sequence .= "_$primaryKey";
            }
            $sequence .= '_seq';
            return ibase_gen_id($sequence, $this->_conn, 0);
        }

        return null;
    }

    public function prepare($prepareString) {
        $stmt = @ibase_prepare($this->_conn, $prepareString);
        if (!$stmt) {
            throw new IbaseException(ibase_errmsg());
        }
        return new IbaseStatement($stmt);
    }

    public function query() {
        $args = func_get_args();
        $sql = $args[0];
        $stmt = $this->prepare($sql);
        $stmt->execute();
        return $stmt;
    }

    public function quote($input, $type = \PDO::PARAM_STR) {
        if ($type == \PDO::PARAM_INT) {
            return $input;
        }

        $input = str_replace("'", "''", $input);
        return "'" . $input . "'";
    }

    public function rollBack() {
        if (!ibase_commit($this->_transResource ? $this->_transResource : $this->_conn)) {
            throw new IbaseException(ibase_errmsg());
        }
    }

}
