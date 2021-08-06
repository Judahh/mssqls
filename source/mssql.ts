import { PoolAdapter } from '@flexiblepersistence/dao';
import { ConnectionPool } from 'mssql';
import { PersistenceInfo } from 'flexiblepersistence';
import SqlString from 'tsqlstring';

export class MSSQL implements PoolAdapter {
  protected pool: ConnectionPool;
  protected persistenceInfo: PersistenceInfo;
  constructor(persistenceInfo: PersistenceInfo) {
    this.persistenceInfo = persistenceInfo;
    this.pool = new ConnectionPool(this.persistenceInfo);
  }
  public getPersistenceInfo(): PersistenceInfo {
    return this.persistenceInfo;
  }
  public connect(callback: unknown): Promise<unknown> {
    return this.pool.connect(callback);
  }
  public async query(
    script: string,
    values?: Array<unknown>,
    callback?: () => unknown
  ): Promise<unknown> {
    //! TODO: TEST VALUES
    //! According to the documentation for mssql you can use es6 template literals in you INSERT statement.
    //! EX.: pool.query`INSERT INTO sigfoxmessages (device,data,station,rssi,unix_timestamp) VALUES(${request.payload.device}, ${request.payload.data}, ${request.payload.station}, ${request.payload.rssi}, ${request.payload.time}))`
    const pool = await this.pool.connect();
    script = script.replace(/[$]\d/g, (substring: string) => {
      if (values) {
        let element = values[parseInt(substring.replace('$', '')) - 1];
        element = SqlString.escape(element);
        const isString = typeof element === 'string';
        if (isString) element = "'" + element + "'";
        return element as string;
      }
      return '';
    });
    return pool.request().query(script, callback);
  }
  public end(callback?: () => unknown): Promise<any> {
    return this.pool.close(callback);
  }
}
