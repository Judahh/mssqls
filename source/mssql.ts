import { PoolAdapter } from '@flexiblepersistence/dao';
import { ConnectionPool } from 'mssql';
import { PersistenceInfo } from 'flexiblepersistence';
import SqlString from 'tsqlstring';

export class MSSQL implements PoolAdapter {
  simpleCreate = true;
  simpleUpdate = true;
  simpleDelete = true;
  protected pool: ConnectionPool;
  protected persistenceInfo: PersistenceInfo;
  constructor(persistenceInfo: PersistenceInfo) {
    this.persistenceInfo = persistenceInfo;
    this.pool = new ConnectionPool(this.persistenceInfo);
  }
  validateOptions(options?: {
    page?: number | undefined;
    pageSize?: number | undefined;
    numberOfPages?: number | undefined;
  }): boolean {
    if (options?.pageSize) {
      options.page = options.page || 1;
      return !isNaN(options.page) && !isNaN(options.pageSize);
    }
    return false;
  }
  async getNumberOfPages(
    script: string,
    options?: {
      page?: number | undefined;
      pageSize?: number | undefined;
      numberOfPages?: number | undefined;
    },
    // eslint-disable-next-line no-unused-vars
    reject?: (error: Error) => unknown
  ): Promise<void> {
    if (this.validateOptions(options)) {
      const query = 'SELECT COUNT(*) FROM ( ' + script + ' ) as numberOfPages';
      await this.pool.query(query, (error, results) => {
        if (error && reject) {
          reject(new Error(error));
        } else if (options) {
          options.numberOfPages = results.rows[0];
        }
      });
    }
  }
  async generatePaginationPrefix(options?: {
    page?: number | undefined;
    pageSize?: number | undefined;
    numberOfPages?: number | undefined;
  }): Promise<unknown> {
    let query = '';
    if (this.validateOptions(options)) {
      query =
        ` DECLARE @PageNumber AS INT, @RowsPage AS INT ` +
        `SET @PageNumber = ${options?.page} ` +
        `SET @RowsPage = ${options?.pageSize} ` +
        `SELECT * FROM (SELECT DENSE_RANK() OVER(ORDER BY pagingElement.id) AS elementNumber,* FROM ( `;
    }
    return query;
  }
  async generatePaginationSuffix(options?: {
    page?: number | undefined;
    pageSize?: number | undefined;
    numberOfPages?: number | undefined;
  }): Promise<unknown> {
    let query = '';
    if (this.validateOptions(options)) {
      query =
        `) as pagingElement) as newPagingElement WHERE ` +
        `elementNumber BETWEEN((@PageNumber - 1) * @RowsPage + 1) ` +
        `AND (@PageNumber * @RowsPage) `;
    }
    return query;
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
    script = script.replace(/[$]\d*/g, (substring: string) => {
      if (values) {
        return SqlString.escape(
          values[parseInt(substring.replace('$', '')) - 1]
        ) as string;
      }
      return '';
    });
    return pool.request().query(script, callback);
  }
  public end(callback?: () => unknown): Promise<any> {
    return this.pool.close(callback);
  }
}
