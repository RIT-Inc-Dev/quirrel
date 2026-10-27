// Parcel's `url:` pipeline resolves to the asset URL
declare module "url:*" {
  const url: string;
  export default url;
}
