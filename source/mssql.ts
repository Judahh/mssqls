import { IPool } from '@flexiblepersistence/dao';
import { ConnectionPool } from 'mssql';
import { IEventOptions, PersistenceInfo } from 'flexiblepersistence';
import SqlString from 'tsqlstring';
import { Transaction } from './transaction';

export class MSSQL implements IPool {
  simpleCreate = true;
  simpleUpdate = true;
  isCreateLimitBefore = true;
  isReadLimitBefore = true;
  isUpdateLimitBefore = true;
  isDeleteLimitBefore = true;
  createLimit = 'TOP';
  readLimit = 'TOP';
  updateLimit = 'TOP';
  deleteLimit = 'TOP';
  protected pool: ConnectionPool;
  protected persistenceInfo: PersistenceInfo;
  constructor(persistenceInfo: PersistenceInfo) {
    this.persistenceInfo = persistenceInfo;
    this.pool = new ConnectionPool(this.persistenceInfo);
  }
  validateOptions(options?: IEventOptions): boolean {
    if (options) {
      options.pageSize = options?.pageSize || options?.pagesize;
      if (options?.pageSize !== undefined && options?.pageSize !== null) {
        options.page =
          options?.page !== undefined && options?.page !== null
            ? parseInt(options?.page?.toString())
            : 0;
        options.pageSize = parseInt(options?.pageSize?.toString());
        return (
          !isNaN(options?.page) &&
          !isNaN(options?.pageSize as unknown as number)
        );
      }
    }
    return false;
  }
  async getPages(
    script: string,
    values?: Array<unknown>,
    options?: IEventOptions,
    idName?: string
  ): Promise<number> {
    if (options && this.validateOptions(options)) {
      let elementNumber =
        (!options?.noDenseRank && idName !== undefined
          ? 'DENSE_RANK() OVER(ORDER BY ' + idName + ')'
          : options?.useRowNumber && idName !== undefined
          ? 'ROW_NUMBER() OVER (ORDER BY ' + idName + ')'
          : 'COUNT(*)') + ' AS elementNumber';
      const distinct = !options?.noDistinct ? 'distinct ' : '';
      const addParam = idName ? ',' + idName : '';
      elementNumber = 'SELECT ' + distinct + ' ' + elementNumber + addParam;
      const query =
        'SELECT COUNT(*) FROM ( ' +
        elementNumber +
        ' FROM (' +
        script +
        ' ) as pagingElement' +
        ' ) as pages';
      const results = await this.query(query, values);
      if (options?.pageSize && results?.recordset && results?.recordset[0]) {
        const rows = results.recordset[0][''];
        options.pages = Math.ceil(
          rows / parseInt(options?.pageSize?.toString())
        );
      }
    }
    return parseInt((options?.pages || 1).toString());
  }
  async generatePaginationPrefix(
    options?: IEventOptions,
    idName?: string
  ): Promise<string> {
    let query = '';
    if (this.validateOptions(options)) {
      let elementNumber =
        (!options?.noDenseRank
          ? 'DENSE_RANK() OVER(ORDER BY ' + idName + ')'
          : options?.useRowNumber && idName !== undefined
          ? 'ROW_NUMBER() OVER (ORDER BY ' + idName + ')'
          : 'COUNT(*)') + ' AS elementNumber';
      const distinct = !options?.noDistinct ? 'distinct ' : '';
      const addParam = ',*';
      elementNumber = 'SELECT ' + distinct + ' ' + elementNumber + addParam;
      query =
        ` DECLARE @PageNumber AS INT, @RowsPage AS INT ` +
        `SET @PageNumber = ${options?.page} ` +
        `SET @RowsPage = ${options?.pageSize} ` +
        `SELECT * FROM (${elementNumber} FROM ( `;
    }
    return query;
  }
  async generatePaginationSuffix(options?: IEventOptions): Promise<string> {
    let query = '';
    if (this.validateOptions(options)) {
      query =
        `) as pagingElement) as newPagingElement WHERE ` +
        `elementNumber BETWEEN(@PageNumber * @RowsPage + 1) ` +
        `AND ((@PageNumber + 1) * @RowsPage) `;
    }
    return query;
  }
  public getPersistenceInfo(): PersistenceInfo {
    return this.persistenceInfo;
  }
  public connect(): Promise<boolean> {
    return this.pool.connect();
  }
  public async query(
    script: string,
    values?: Array<unknown>
  ): Promise<{
    rows?: Array<unknown>;
    rowCount?: number;
    rowsAffected?: number[];
    recordset?: any;
  }> {
    //! TODO: TEST VALUES
    //! According to the documentation for mssql you can use es6 template literals in you INSERT statement.
    //! EX.: pool.query`INSERT INTO sigfoxmessages (device,data,station,rssi,unix_timestamp) VALUES(${request.payload.device}, ${request.payload.data}, ${request.payload.station}, ${request.payload.rssi}, ${request.payload.time}))`
    const pool = await this.pool.connect();
    script = script.replace(/[$]\d*/g, (substring: string) => {
      if (values) {
        let value = values[parseInt(substring.replace('$', '')) - 1];
        if (Array.isArray(value)) {
          value = '(' + value.map((a) => SqlString.escape(a)).join(',') + ')';
          return value as string;
        }
        return SqlString.escape(value) as string;
      }
      return '';
    });
    if (JSON.parse((process?.env?.DAO_MSSQL_LOG || 'false').toLowerCase())) {
      console.log('MSSQL QUERY:');
      console.log(script);
    }
    return pool.request().query(script);
  }
  public async begin(options?): Promise<Transaction> {
    const t = new Transaction(this.pool);
    await t.begin(options);
    return t;
  }
  public async commit(transaction: Transaction): Promise<void> {
    await transaction.commit();
  }
  public async rollback(transaction: Transaction): Promise<void> {
    await transaction.rollback();
  }
  public end(): Promise<boolean> {
    return this.pool.close();
  }
}
