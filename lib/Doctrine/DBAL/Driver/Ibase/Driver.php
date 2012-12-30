<?php

namespace Doctrine\DBAL\Driver\Ibase;

use Doctrine\DBAL\Connection;

class Driver implements \Doctrine\DBAL\Driver {

    public function connect(array $params, $username = null, $password = null, array $driverOptions = array()) {
        return new IbaseConnection($params, $username, $password, $driverOptions);
    }

    /**
     * {@inheritdoc}
     */
    public function getDatabase(Connection $conn)
    {
        $params = $conn->getParams();
        return $params['dbname'];
    }

    /**
     * {@inheritdoc}
     */
    public function getDatabasePlatform()
    {
        return new \Doctrine\DBAL\Platforms\IbasePlatform();
    }

    public function getName() {
        return 'ibase';
    }

    /**
     * {@inheritdoc}
     */
    public function getSchemaManager(\Doctrine\DBAL\Connection $conn)
    {
        return new \Doctrine\DBAL\Schema\IbaseSchemaManager($conn);
    }

}
