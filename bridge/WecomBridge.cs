using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Drawing;
using System.Drawing.Drawing2D;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Web.Script.Serialization;
using System.Windows.Forms;
using Map = System.Collections.Generic.Dictionary<string,object>;

internal static class Bridge
{
    internal static readonly string Root = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile),"Wecom Assistant","bridge");
    internal static readonly UTF8Encoding Utf8 = new UTF8Encoding(false);
    internal static Map Parse(string text) { return new JavaScriptSerializer { MaxJsonLength=8388608 }.Deserialize<Map>(text); }
    internal static string Json(object obj) { return new JavaScriptSerializer().Serialize(obj); }
    internal static Map Read(string name)
    {
        try { return Parse(File.ReadAllText(Path.Combine(Root,name),Utf8)); }
        catch (IOException) { return new Map(); }
        catch (ArgumentException) { return new Map(); }
        catch (InvalidOperationException) { return new Map(); }
    }
    internal static object Get(Map m,string k) { object v; return m!=null && m.TryGetValue(k,out v) ? v : null; }
    internal static string Text(Map m,string k) { return Convert.ToString(Get(m,k)); }
    internal static bool True(Map m,string k) { return Object.Equals(Get(m,k),true); }
    internal static bool Fresh(Map m,int seconds)
    {
        DateTimeOffset dt;
        if (!DateTimeOffset.TryParse(Text(m,"updated_at"),out dt)) return false;
        double age=(DateTimeOffset.UtcNow-dt).TotalSeconds;
        return age>=-1 && age<seconds;
    }
    internal static async Task<Map> Call(string action, object input)
    {
        string python=Text(Read("panel-runtime.json"),"python");
        if (!Path.IsPathRooted(python) || !File.Exists(python)) throw new IOException("找不到已配置的 Python 运行时");
        var info=new ProcessStartInfo(python,"\""+Path.Combine(Root,"panel_backend.py")+"\" "+action) {
            WorkingDirectory=Root,UseShellExecute=false,CreateNoWindow=true,
            RedirectStandardOutput=true,RedirectStandardError=true,RedirectStandardInput=true,
            StandardOutputEncoding=Utf8,StandardErrorEncoding=Utf8
        };
        info.EnvironmentVariables["PYTHONIOENCODING"]="utf-8";
        using(var process=Process.Start(info))
        {
            Task<string> output=process.StandardOutput.ReadToEndAsync();
            Task<string> error=process.StandardError.ReadToEndAsync();
            if(input!=null) {
                byte[] bytes=Utf8.GetBytes(Json(input));
                await process.StandardInput.BaseStream.WriteAsync(bytes,0,bytes.Length);
            }
            process.StandardInput.Close();
            await Task.WhenAll(output,error);
            await Task.Run((Action)process.WaitForExit);
            if(process.ExitCode!=0) throw new IOException("桥接服务未完成操作，请查看运行日志");
            return Parse(output.Result);
        }
    }
    internal static void Note(string note)
    {
        try { File.AppendAllText(Path.Combine(Root,"tray-lifecycle.log"),DateTimeOffset.Now.ToString("o")+"  "+note+Environment.NewLine,Utf8); }
        catch(IOException) {}
        catch(UnauthorizedAccessException) {}
    }
    [STAThread] internal static int Main(string[] args)
    {
        bool first;
        using(var mutex=new Mutex(true,@"Local\XiaoaiWecomNativePanel",out first))
        using(var recheck=new EventWaitHandle(false,EventResetMode.AutoReset,@"Local\XiaoaiWecomTrayRecheck"))
        {
            if(!first) { if(Array.IndexOf(args,"--login")<0) recheck.Set(); return 0; }
            Application.EnableVisualStyles();
            Application.SetCompatibleTextRenderingDefault(false);
            Application.SetUnhandledExceptionMode(UnhandledExceptionMode.CatchException);
            Application.ThreadException+=(s,e)=>Note("界面异常 "+e.Exception.GetType().Name);
            SynchronizationContext.SetSynchronizationContext(new WindowsFormsSynchronizationContext());
            try { using(var app=new TrayHost(recheck, Array.IndexOf(args,"--restore-panel") >= 0)) Application.Run(app); }
            catch(Exception e) { Note("启动失败 "+e.GetType().Name); return 1; }
            finally { mutex.ReleaseMutex(); }
        }
        return 0;
    }
}

internal sealed class TrayHost : ApplicationContext
{
    [DllImport("user32.dll")] static extern bool DestroyIcon(IntPtr handle);
    readonly NotifyIcon tray=new NotifyIcon();
    readonly System.Windows.Forms.Timer timer=new System.Windows.Forms.Timer();
    readonly EventWaitHandle recheck;
    readonly Icon[] icons={Fish(Color.FromArgb(111,123,138)),Fish(Color.FromArgb(213,141,29)),Fish(Color.FromArgb(34,161,130))};
    BridgeWindow panel;
    internal bool Starting { get; private set; }
    internal string Health { get; private set; }
    internal string Operation { get; private set; }
    string previous="";
    DateTime nextRefresh=DateTime.MinValue, auditRefresh=DateTime.MinValue;
    DateTime? unhealthySince=null;
    DateTime nextAutoRecover=DateTime.MinValue;
    int autoRecoverAttempts=0;
    string lastDataVersion="";
    string DataVersion()
    {
        return String.Join("|", new[]{"business.sqlite3-wal","business.sqlite3","local-tasks.sqlite3-wal","local-tasks.sqlite3","queue","t3-bridge-config.json"}.Select(name=>File.GetLastWriteTimeUtc(Path.Combine(Bridge.Root,name)).Ticks.ToString()).ToArray());
    }
    bool localHealthy, servicesHealthy;
    void Heartbeat(bool running)
    {
        bool visible=running && panel!=null && panel.Visible;
        string target=Path.Combine(Bridge.Root,"tray_ui_state.json"), temp=target+".tray-new";
        try {
            File.WriteAllText(temp,Bridge.Json(new {pid=Process.GetCurrentProcess().Id,updated_at=DateTimeOffset.UtcNow.ToString("o"),
                tray_visible=running,window_visible=visible,state=running?"running":"stopped",health=previous,engine="t3"}),Bridge.Utf8);
            if(File.Exists(target))File.Replace(temp,target,null);else File.Move(temp,target);
        } catch(IOException) {} catch(UnauthorizedAccessException) {}
    }
    static Icon Fish(Color accent)
    {
        using(var bmp=new Bitmap(32,32))
        using(var g=Graphics.FromImage(bmp))
        using(var fill=new SolidBrush(Color.FromArgb(29,54,78)))
        using(var status=new SolidBrush(accent))
        {
            g.SmoothingMode=SmoothingMode.AntiAlias;
            g.FillEllipse(fill,2,6,23,18);
            g.FillPolygon(fill,new[]{new Point(21,15),new Point(30,7),new Point(30,23)});
            g.FillEllipse(Brushes.White,7,11,4,4);
            g.FillEllipse(Brushes.White,18,18,14,14);
            g.FillEllipse(status,20,20,10,10);
            IntPtr handle=bmp.GetHicon();
            try { using(var original=Icon.FromHandle(handle)) return (Icon)original.Clone(); }
            finally { DestroyIcon(handle); }
        }
    }
    readonly bool restorePanel;
    static bool ConsumeRecoveryRequest()
    {
        string p=Path.Combine(Bridge.Root,"recovery.request");
        if(!File.Exists(p)) return false;
        try { File.Delete(p); } catch(IOException) {} catch(UnauthorizedAccessException) {}
        return true;
    }
    static bool ManuallyStopped()
    {
        return File.Exists(Path.Combine(Bridge.Root,"stop.request"))
            || File.Exists(Path.Combine(Bridge.Root,"bridge-daemon.stop"))
            || File.Exists(Path.Combine(Bridge.Root,"watchdog.stop"));
    }
    internal TrayHost(EventWaitHandle signal, bool showPreviousPanel)
    {
        recheck=signal;
        restorePanel=showPreviousPanel;
        Health="正在检查"; Operation="";
        tray.Icon=icons[0]; tray.Text="大肥鱼 T3 消息桥 · 正在检查";
        tray.MouseClick+=(s,e)=>{ if(e.Button==MouseButtons.Left || e.Button==MouseButtons.Right) Reveal(); };
        tray.Visible=true;
        timer.Interval=1000;
        timer.Tick+=async(s,e)=>{
            Tick();
            bool requested=recheck.WaitOne(0) | ConsumeRecoveryRequest();
            if(requested) {
                if(!Starting) {
                    Bridge.Note("收到外部服务恢复请求，正在独立会话中拉起缺失服务");
                    await Recover();
                }
                return;
            }
            if(!servicesHealthy && !Starting && !ManuallyStopped()) {
                var now=DateTime.UtcNow;
                if(!unhealthySince.HasValue) unhealthySince=now;
                if((now-unhealthySince.Value).TotalSeconds>=25 && now>=nextAutoRecover) {
                    autoRecoverAttempts++;
                    int delay=Math.Min(300, 30 * (1 << Math.Min(autoRecoverAttempts - 1, 4)));
                    nextAutoRecover=now.AddSeconds(delay);
                    Bridge.Note("检测到桥接服务持续异常，托盘自动执行第 "+autoRecoverAttempts+" 次自愈恢复");
                    await Recover();
                }
            }
        };
        timer.Start();
        Tick();
        Bridge.Note("T3 托盘已启动");
        Application.Idle+=FirstIdle;
    }
    async void FirstIdle(object sender,EventArgs e)
    {
        Application.Idle-=FirstIdle;
        if(restorePanel) Reveal();
        await Recover();
    }
    static bool Receiver(Map s) { return Bridge.Fresh(s,15) && Bridge.Text(s,"state")=="authenticated"; }
    bool Healthy() { return localHealthy; }
    void Tick()
    {
        var rx=Bridge.Read("status.json");
        var pb=Bridge.Read("private-bot-status.json");
        bool pbConfigured=File.Exists(Path.Combine(Bridge.Root,"private-bot-credentials.dpapi"));
        var daemon=Bridge.Read("bridge-daemon-status.json");
        var dispatchMap=Bridge.Read("local-dispatch-status.json");
        var guard=Bridge.Read("watchdog-state.json");
        var cfg=Bridge.Read("t3-bridge-config.json");
        bool r=Receiver(rx);
        bool pbOk=!pbConfigured || Receiver(pb);
        bool d=Bridge.Fresh(daemon,25) && Bridge.Text(daemon,"state")=="running";
        bool t3=d && Bridge.True(daemon,"t3_server_ok");
        bool dispatch=Bridge.Fresh(dispatchMap,25) && Bridge.True(cfg,"enabled");
        bool w=Bridge.Fresh(guard,35) && String.IsNullOrEmpty(Bridge.Text(guard,"stopped_at"));
        servicesHealthy=r && pbOk && d && dispatch && w;
        localHealthy=servicesHealthy && t3;
        if(servicesHealthy) {
            unhealthySince=null; autoRecoverAttempts=0; nextAutoRecover=DateTime.MinValue;
        }
        string state=localHealthy ? "收件与 T3 调度就绪" : (r || d || dispatch ? "部分服务待检查" : "离线");
        string pbLabel=pbConfigured ? ("   克劳德 "+(pbOk?"在线":"待检查")) : "";
        Health="企微收件 "+(r?"在线":"待检查")+pbLabel+"   T3 服务 "+(t3?"就绪":"待启动")+"   调度引擎 "+(dispatch?"心跳正常":"待检查")+"   守护 "+(w?"运行中":"待检查");
        if(state!=previous) {
            tray.Icon=icons[localHealthy?2:state=="离线"?0:1];
            tray.Text="大肥鱼 T3 消息桥 · "+state;
            Bridge.Note(state+"  "+Health); previous=state;
        }
        if(panel!=null && panel.Visible) {
            panel.SetStatus();
            if(DateTime.UtcNow>=nextRefresh) {
                var now=DateTime.UtcNow; nextRefresh=now.AddSeconds(2);
                string version=DataVersion();
                if(version!=lastDataVersion || now>=auditRefresh) {
                    lastDataVersion=version; auditRefresh=now.AddSeconds(60); panel.RefreshData();
                }
            }
        }
        Heartbeat(true);
    }
    internal async Task Recover()
    {
        if(Starting) return;
        Starting=true; Operation="正在检查服务…";
        if(panel!=null) panel.SetStatus();
        try {
            var data=await Bridge.Call("start",null);
            foreach(Map result in (IEnumerable)Bridge.Get(data,"results"))
                if(Convert.ToInt32(Bridge.Get(result,"exit"))!=0) throw new IOException("服务恢复未完成");
            Operation="服务检查完成";
        } catch(Exception e) { Operation=e.Message; }
        finally { Starting=false; Bridge.Note(Operation); Tick(); }
    }
    void Reveal()
    {
        if(panel==null || panel.IsDisposed) panel=new BridgeWindow(this);
        panel.Show(); panel.WindowState=FormWindowState.Normal; panel.Activate(); Heartbeat(true);
        panel.SetStatus(); panel.RefreshData(); nextRefresh=DateTime.UtcNow.AddSeconds(2);
    }
    protected override void Dispose(bool disposing)
    {
        if(disposing) {
            Application.Idle-=FirstIdle; timer.Dispose(); tray.Visible=false; tray.Dispose();
            if(panel!=null) panel.Dispose();
            foreach(var icon in icons) icon.Dispose();
            Heartbeat(false); Bridge.Note("T3 托盘已退出");
        }
        base.Dispose(disposing);
    }
}

internal sealed class BridgeWindow : Form
{
    readonly TrayHost host;
    readonly Label status=new Label(), feedback=new Label(), modelNote=new Label(), conclusionNote=new Label();
    readonly ListView history=List("时间","事件","反馈","消息 / 任务");
    readonly ListView jobs=List("创建时间","状态","路由 / 模型","T3 线程 ID");
    readonly ListView cases=CaseList();
    readonly ListView notesList=NoteList("编号","标题","群会话尾号","更新时间");
    readonly ListView followupsList=NoteList("编号","状态","同事追问内容","群会话尾号");
    readonly TextBox detail=new TextBox{Dock=DockStyle.Fill,Multiline=true,ReadOnly=true,ScrollBars=ScrollBars.Both,WordWrap=true,BorderStyle=BorderStyle.None};
    string selectedCase="", selectedLink="", selectedWorkspace="";
    int detailVersion;
    readonly ComboBox entryModel=ModelPicker(), escModel=ModelPicker();
    readonly ComboBox entryEffort=Effort(), escEffort=Effort();
    readonly ComboBox clarMode=ModelPicker();
    bool busy, loaded, bindingModels;
    string catalogKey="", savedEntry="", savedEsc="", savedClar="";

    sealed class Option
    {
        internal string Provider, Id, Label, Default;
        internal string[] Efforts;
        public override string ToString(){ return Label + " [" + Provider + "]"; }
    }
    static ComboBox ModelPicker(){ return new ComboBox{DropDownStyle=ComboBoxStyle.DropDownList,MaxDropDownItems=14,IntegralHeight=false,DropDownHeight=300}; }
    static Option SelectedOption(ComboBox combo){ return combo.SelectedItem as Option; }
    static string SelectedEffort(ComboBox combo){ return Convert.ToString(combo.SelectedItem); }
    static string[] Supported={"low","medium","high","xhigh","max","ultra"};

    void BindEfforts(ComboBox picker, ComboBox effort, string preferred)
    {
        var model=SelectedOption(picker);
        var levels=model==null ? new string[0] : model.Efforts;
        effort.BeginUpdate();
        try {
            effort.Items.Clear(); effort.Items.AddRange(levels); effort.Enabled=levels.Length>0;
            if(levels.Length>0) effort.SelectedItem=levels.Contains(preferred) ? preferred : levels.Contains(model.Default) ? model.Default : levels[0];
        } finally { effort.EndUpdate(); }
    }
    void BindModels(ComboBox picker, List<Option> options, string provider, string modelId)
    {
        picker.BeginUpdate();
        try {
            picker.Items.Clear();
            picker.Items.AddRange(options.ToArray());
            var saved=options.FirstOrDefault(o=>o.Id==modelId && (String.IsNullOrEmpty(provider) || o.Provider==provider));
            if(saved!=null) picker.SelectedItem=saved;
            else if(options.Count>0) picker.SelectedIndex=0;
        } finally { picker.EndUpdate(); }
    }
    static string PrefKey(string p, string m, string e){ return p+"|"+m+"|"+e; }
    void SyncModels(Map prefs, object catalog)
    {
        if(entryModel.DroppedDown || escModel.DroppedDown || entryEffort.DroppedDown || escEffort.DroppedDown || clarMode.DroppedDown) return;
        var rows=catalog as IEnumerable;
        if(rows==null) return;
        var options=new List<Option>();
        foreach(var row in rows.OfType<Map>()) {
            var raw=Bridge.Get(row,"efforts") as IEnumerable;
            if(raw==null) continue;
            var levels=raw.OfType<string>().Where(x=>Supported.Contains(x)).Distinct().ToArray();
            if(levels.Length>0) options.Add(new Option{
                Provider=Bridge.Text(row,"provider"), Id=Bridge.Text(row,"id"),
                Label=Bridge.Text(row,"label"), Default=Bridge.Text(row,"default"), Efforts=levels
            });
        }
        if(options.Count==0){ modelNote.Text="T3 模型目录暂不可用。"; return; }
        string version=String.Join("\n",options.Select(o=>o.Provider+"|"+o.Id+"|"+String.Join(",",o.Efforts)));
        var em=Bridge.Get(prefs,"entry_model") as Map;
        var sm=Bridge.Get(prefs,"escalation_model") as Map;
        string ep=Bridge.Text(em,"provider"), ei=Bridge.Text(em,"model"), ee=Bridge.Text(em,"effort");
        string sp=Bridge.Text(sm,"provider"), si=Bridge.Text(sm,"model"), se=Bridge.Text(sm,"effort");
        string cm=Bridge.Text(prefs,"clarification_mode"); if(String.IsNullOrEmpty(cm)) cm="strict";
        string peKey=PrefKey(ep,ei,ee), psKey=PrefKey(sp,si,se);
        if(loaded && version==catalogKey && savedEntry==peKey && savedEsc==psKey && savedClar==cm) return;
        bindingModels=true;
        try {
            BindModels(entryModel,options,ep,ei); BindEfforts(entryModel,entryEffort,ee);
            BindModels(escModel,options,sp,si); BindEfforts(escModel,escEffort,se);
            clarMode.SelectedIndex = cm=="standard" ? 1 : 0;
        } finally { bindingModels=false; }
        catalogKey=version; savedEntry=peKey; savedEsc=psKey; savedClar=cm; loaded=true;
        Controls.Find("saveModels",true)[0].Enabled=true;
    }

    static ListView List(params string[] names)
    {
        var view=new ListView{Dock=DockStyle.Fill,View=View.Details,FullRowSelect=true,HideSelection=false,BorderStyle=BorderStyle.None};
        int[] widths={150,115,360,360};
        for(int i=0;i<names.Length;i++) view.Columns.Add(names[i],widths[i]);
        return view;
    }
    static ListView NoteList(params string[] names)
    {
        var view=new ListView{Dock=DockStyle.Fill,View=View.Details,FullRowSelect=true,HideSelection=false,MultiSelect=false,BorderStyle=BorderStyle.None};
        int[] widths={100,260,360,160};
        for(int i=0;i<names.Length;i++) view.Columns.Add(names[i],widths[i]);
        return view;
    }
    static ListView CaseList()
    {
        var view=new ListView{Dock=DockStyle.Fill,View=View.Details,FullRowSelect=true,HideSelection=false,MultiSelect=false,BorderStyle=BorderStyle.None};
        string[] names={"更新时间","业务事项","状态","路由 / 模型","消息 / 文件","T3 线程"};
        int[] widths={140,260,95,210,85,245};
        for(int i=0;i<names.Length;i++) view.Columns.Add(names[i],widths[i]);
        return view;
    }
    static ComboBox Effort()
    {
        var c=new ComboBox{DropDownStyle=ComboBoxStyle.DropDownList,Width=130};
        c.Items.AddRange(new object[]{"low","medium","high","xhigh","max","ultra"}); c.SelectedItem="high"; return c;
    }
    static void Place(Control parent, Control child,int x,int y,int w,int h)
    { child.SetBounds(x,y,w,h); parent.Controls.Add(child); }
    static Label Caption(string value) { return new Label{Text=value,AutoSize=false,TextAlign=ContentAlignment.MiddleLeft}; }

    internal BridgeWindow(TrayHost parent)
    {
        host=parent; Text="大肥鱼 T3 消息桥"; Font=new Font("Microsoft YaHei UI",9.5F);
        AutoScaleMode=AutoScaleMode.Dpi; ClientSize=new Size(1120,760); MinimumSize=new Size(920,650);
        StartPosition=FormStartPosition.CenterScreen; ShowInTaskbar=false; BackColor=Color.White;
        var title=Caption("大肥鱼 T3 消息桥"); title.Font=new Font(Font.FontFamily,20,FontStyle.Bold);
        Place(this,title,24,12,480,48); Place(this,status,27,74,1060,28);
        status.Anchor=AnchorStyles.Top|AnchorStyles.Left|AnchorStyles.Right;
        status.ForeColor=Color.FromArgb(25,88,95);
        var tabs=new TabControl{Anchor=AnchorStyles.Top|AnchorStyles.Bottom|AnchorStyles.Left|AnchorStyles.Right};
        Place(this,tabs,24,112,1072,570);

        // Tab 1: 业务事项 (no Dot column!)
        var casePage=new TabPage("业务事项");
        var split=new SplitContainer{Dock=DockStyle.Fill,Orientation=Orientation.Horizontal,SplitterWidth=7};
        var caseTop=new Panel{Dock=DockStyle.Fill};
        cases.Dock=DockStyle.Fill; caseTop.Controls.Add(cases);
        var caseBar=new FlowLayoutPanel{Dock=DockStyle.Top,Height=34,Padding=new Padding(2,2,2,2)};
        var openThreadBtn=new Button{Text="打开 T3 线程",Width=110,Height=28,Enabled=false};
        var openFolderBtn=new Button{Text="打开任务目录",Width=110,Height=28,Enabled=false};
        openThreadBtn.Click+=(s,e)=>{ if(!String.IsNullOrEmpty(selectedLink)) try { Process.Start(selectedLink); } catch {} };
        openFolderBtn.Click+=(s,e)=>{ if(!String.IsNullOrEmpty(selectedWorkspace) && Directory.Exists(selectedWorkspace)) try { Process.Start("explorer.exe",selectedWorkspace); } catch {} };
        caseBar.Controls.AddRange(new Control[]{openThreadBtn,openFolderBtn});
        split.Panel1.Controls.Add(caseTop);
        split.Panel2.Controls.Add(detail); split.Panel2.Controls.Add(caseBar);
        casePage.Controls.Add(split);
        split.Size=new Size(1060,530); split.SplitterDistance=245;
        cases.SelectedIndexChanged+=async(s,e)=>{
            if(cases.SelectedItems.Count!=1) return;
            var row=cases.SelectedItems[0].Tag as Map;
            selectedCase=Bridge.Text(row,"id");
            selectedLink=Bridge.Text(row,"t3_link");
            selectedWorkspace=Bridge.Text(row,"workspace");
            openThreadBtn.Enabled=!String.IsNullOrEmpty(selectedLink);
            openFolderBtn.Enabled=!String.IsNullOrEmpty(selectedWorkspace) && Directory.Exists(selectedWorkspace);
            await ShowCase(row);
        };

        // Tab 2 & 3
        var logPage=new TabPage("历史时间线"); logPage.Controls.Add(history);
        var tasksPage=new TabPage("T3 执行记录"); tasksPage.Controls.Add(jobs);
        jobs.DoubleClick+=(s,e)=>{
            if(jobs.SelectedItems.Count!=1) return;
            var row=jobs.SelectedItems[0].Tag as Map;
            var link=Bridge.Text(row,"t3_link");
            var ws=Bridge.Text(row,"workspace");
            if(!String.IsNullOrEmpty(link)) try { Process.Start(link); return; } catch {}
            if(!String.IsNullOrEmpty(ws) && Directory.Exists(ws)) try { Process.Start("explorer.exe",ws); } catch {}
        };

        // Tab 4: 模型与路由设置 (replaces old 默认模型 + 消息分流)
        var settingsPage=new TabPage("模型与路由设置"){BackColor=Color.White};
        Place(settingsPage,Caption("默认轻量入口模型（每条新消息首先由此模型做清晰度门禁与复杂度判断）"),24,18,760,26);
        Place(settingsPage,entryModel,24,48,460,32); Place(settingsPage,entryEffort,500,47,145,32);
        Place(settingsPage,Caption("复杂任务升级主控模型（Hydrogen 判为复杂时转交此模型择优执行或组织多 Agent 讨论）"),24,98,780,26);
        Place(settingsPage,escModel,24,128,460,32); Place(settingsPage,escEffort,500,127,145,32);
        Place(settingsPage,Caption("模糊 / 矛盾指令追问策略"),24,178,460,26);
        clarMode.Items.AddRange(new object[]{"严格防猜想（有关键不确定性先追问，推荐）","标准模式（明显矛盾或高风险缺口先追问）"});
        Place(settingsPage,clarMode,24,208,460,32);
        entryModel.SelectedIndexChanged+=(s,e)=>{ if(!bindingModels) BindEfforts(entryModel,entryEffort,SelectedEffort(entryEffort)); };
        escModel.SelectedIndexChanged+=(s,e)=>{ if(!bindingModels) BindEfforts(escModel,escEffort,SelectedEffort(escEffort)); };
        var save=new Button{Text="保存设置",Enabled=false,Name="saveModels"};
        Place(settingsPage,save,24,264,135,38);
        modelNote.TextAlign=ContentAlignment.MiddleLeft; Place(settingsPage,modelNote,175,263,680,40);
        Place(settingsPage,Caption("提示：已彻底移除 Dot/小爱与【agent】路由。若消息中显式指定模型（如【模型：gpt-6.2-astra max】或“用 neon”），将直接按指定模型与档位执行。"),24,322,940,36);
        save.Click+=async(s,e)=>{
            save.Enabled=false;
            try {
                var eOpt=SelectedOption(entryModel); var sOpt=SelectedOption(escModel);
                if(eOpt==null || sOpt==null) throw new InvalidOperationException("请选择入口与升级模型");
                await Bridge.Call("save_t3_settings", new {
                    entry_model=new {provider=eOpt.Provider, model=eOpt.Id, effort=SelectedEffort(entryEffort)},
                    escalation_model=new {provider=sOpt.Provider, model=sOpt.Id, effort=SelectedEffort(escEffort)},
                    clarification_mode=clarMode.SelectedIndex==1 ? "standard" : "strict"
                });
                modelNote.Text="已保存，对后续新消息立即生效。";
            } catch(Exception ex){ modelNote.Text="保存失败 · "+ex.Message; }
            finally { save.Enabled=true; }
        };

        // Tab 5: 群结论说明 (Zero-LLM colleague supplementary notes)
        var conclusionPage=new TabPage("群结论说明（零模型）"){BackColor=Color.White};
        var banner=Caption("合规说明：群友查询仅检索主人预先发布的本群静态结论说明卡，全程零 Gemini/OpenAI 模型调用，彻底避免个人订阅反代风险。");
        banner.ForeColor=Color.FromArgb(20,105,70);
        Place(conclusionPage,banner,16,10,1020,26);
        Place(conclusionPage,Caption("已发布的静态结论说明（群内发送「@大肥鱼 【发布说明】」登记，同事发「@大肥鱼 【说明】编号」或引用原结论领取）"),16,42,820,24);
        var notesHost=new Panel{Location=new Point(16,68),Size=new Size(1024,185),Anchor=AnchorStyles.Top|AnchorStyles.Left|AnchorStyles.Right,BorderStyle=BorderStyle.FixedSingle};
        notesHost.Controls.Add(notesList); conclusionPage.Controls.Add(notesHost);
        var delNoteBtn=new Button{Text="停用选中说明",Location=new Point(16,259),Size=new Size(120,30)};
        delNoteBtn.Click+=async(s,e)=>{
            if(notesList.SelectedItems.Count!=1) return;
            var nid=notesList.SelectedItems[0].Text;
            try { await Bridge.Call("delete_conclusion",new {note_id=nid}); conclusionNote.Text="已停用 "+nid; RefreshData(); }
            catch(Exception ex){ conclusionNote.Text="操作失败 · "+ex.Message; }
        };
        conclusionPage.Controls.Add(delNoteBtn);
        Place(conclusionPage,Caption("同事新追问登记（仅本地记录，由你本人审阅后在群里直接回答或更新说明卡）"),16,298,720,24);
        var followHost=new Panel{Location=new Point(16,324),Size=new Size(1024,165),Anchor=AnchorStyles.Top|AnchorStyles.Bottom|AnchorStyles.Left|AnchorStyles.Right,BorderStyle=BorderStyle.FixedSingle};
        followHost.Controls.Add(followupsList); conclusionPage.Controls.Add(followHost);
        var resolveBtn=new Button{Text="标记已人工处理",Location=new Point(16,496),Size=new Size(135,30),Anchor=AnchorStyles.Left|AnchorStyles.Bottom};
        resolveBtn.Click+=async(s,e)=>{
            if(followupsList.SelectedItems.Count!=1) return;
            var fid=Convert.ToString(followupsList.SelectedItems[0].Tag);
            try { await Bridge.Call("resolve_followup",new {id=fid}); conclusionNote.Text="已标记处理完成"; RefreshData(); }
            catch(Exception ex){ conclusionNote.Text="操作失败 · "+ex.Message; }
        };
        conclusionPage.Controls.Add(resolveBtn);
        Place(conclusionPage,conclusionNote,165,496,680,30);
        conclusionNote.Anchor=AnchorStyles.Left|AnchorStyles.Bottom;

        tabs.TabPages.AddRange(new[]{casePage,logPage,tasksPage,settingsPage,conclusionPage});

        var restore=new Button{Text="检查/恢复服务",Anchor=AnchorStyles.Left|AnchorStyles.Bottom};
        Place(this,restore,24,704,124,36); restore.Click+=async(s,e)=>await host.Recover();
        feedback.Anchor=AnchorStyles.Left|AnchorStyles.Right|AnchorStyles.Bottom; feedback.TextAlign=ContentAlignment.MiddleLeft;
        Place(this,feedback,162,704,800,36);
        var hide=new Button{Text="收起",Anchor=AnchorStyles.Right|AnchorStyles.Bottom};
        Place(this,hide,996,704,100,36); hide.Click+=(s,e)=>Hide();
        FormClosing+=(s,e)=>{ if(e.CloseReason==CloseReason.UserClosing){ e.Cancel=true; Hide(); } };
    }

    internal void SetStatus(){ status.Text=host.Health; if(host.Starting) feedback.Text=host.Operation; }
    static string Local(string value)
    {
        DateTimeOffset d; return DateTimeOffset.TryParse(value,out d) ? d.ToLocalTime().ToString("MM-dd HH:mm:ss") : value;
    }
    static string UnixLocal(object value)
    {
        try { return DateTimeOffset.FromUnixTimeSeconds(Convert.ToInt64(Convert.ToDouble(value))).ToLocalTime().ToString("MM-dd HH:mm"); }
        catch { return Convert.ToString(value); }
    }
    static readonly Dictionary<string,string> terms=new Dictionary<string,string> {
        {"received","收到消息"},{"reply","回报"},{"settings_saved","设置更新"},{"services_checked","服务检查"},
        {"pending","待处理"},{"queued","已排队"},{"running","执行中"},{"uncertain","待核实"},
        {"resume_queued","等待轻槽续办"},{"heavy_queued","等待重槽"},{"heavy_resume_queued","等待重槽续办"},
        {"light","轻槽"},{"office","轻槽"},{"regular","重槽"},{"heavy","重槽"},
        {"completed","已完成"},{"blocked","受阻"},{"failed","执行失败"},{"launching","启动中"},
        {"awaiting_clarification","等待确认"},{"colleague_followup","同事待补充"},{"cancelled","已取消"},
        {"collecting","收集中"},{"expired","已过期"},{"historical","历史记录"},{"t3_completed","T3完成"},
        {"t3_escalated","升级高阶模型"},{"clarification_asked","发起追问"},{"t3_worker_started","启动T3任务"}
    };
    static string Friendly(string value)
    {
        if(String.IsNullOrEmpty(value)) return "";
        string[] parts=value.Split(new[]{" / "},StringSplitOptions.None);
        for(int i=0;i<parts.Length;i++){ string label; if(terms.TryGetValue(parts[i],out label)) parts[i]=label; }
        return String.Join(" / ",parts);
    }
    static string Values(Map row,string key)
    {
        var list=Bridge.Get(row,key) as IEnumerable;
        return list==null ? Bridge.Text(row,key) : String.Join(Environment.NewLine,list.Cast<object>().Select(Convert.ToString));
    }
    static string CaseText(Map row)
    {
        var sb=new StringBuilder();
        sb.AppendLine("事项："+Bridge.Text(row,"title"));
        sb.AppendLine("状态："+Friendly(Bridge.Text(row,"state"))+"    路由/模型："+Bridge.Text(row,"model_summary"));
        sb.AppendLine("接收："+Local(Bridge.Text(row,"at"))+"    更新："+Local(Bridge.Text(row,"updated")));
        sb.AppendLine("事项 ID："+Bridge.Text(row,"id"));
        if(!String.IsNullOrEmpty(Bridge.Text(row,"t3_thread_id"))) sb.AppendLine("T3 线程："+Bridge.Text(row,"t3_thread_id"));
        if(!String.IsNullOrEmpty(Bridge.Text(row,"t3_link"))) sb.AppendLine("T3 链接："+Bridge.Text(row,"t3_link"));
        var clars=Bridge.Get(row,"clarifications") as IEnumerable;
        if(clars!=null) {
            var list=clars.OfType<Map>().ToList();
            if(list.Count>0) {
                sb.AppendLine().AppendLine("追问记录：");
                foreach(var c in list)
                    sb.AppendLine("  ["+Bridge.Text(c,"qid")+" · "+Bridge.Text(c,"state")+"] "+Bridge.Text(c,"reason")+" -> 答复："+(String.IsNullOrEmpty(Bridge.Text(c,"answer_text"))?"（待回复）":Bridge.Text(c,"answer_text")));
            }
        }
        sb.AppendLine().AppendLine("文件（"+Bridge.Text(row,"attachment_count")+"）：").AppendLine(Values(row,"files"));
        sb.AppendLine().AppendLine("执行状态：").AppendLine(Bridge.Text(row,"local"));
        sb.AppendLine().AppendLine("证据与日志：").AppendLine(Bridge.Text(row,"evidence"));
        sb.AppendLine().AppendLine("业务结果：").AppendLine(String.IsNullOrEmpty(Bridge.Text(row,"result_text"))?"暂无可读取的业务结果":Bridge.Text(row,"result_text"));
        return sb.ToString();
    }
    async Task ShowCase(Map row)
    {
        int version=++detailVersion; string id=Bridge.Text(row,"id");
        detail.Text=CaseText(row);
        try {
            var full=await Bridge.Call("case_detail",new {id=id});
            if(!IsDisposed && version==detailVersion && selectedCase==id) {
                string text=CaseText(full); if(detail.Text!=text) detail.Text=text;
            }
        } catch(Exception) { if(!IsDisposed && version==detailVersion) detail.AppendText("\r\n\r\n详细结果读取失败，稍后刷新重试。"); }
    }
    void FillCases(object rows)
    {
        var data=rows as IEnumerable; if(data==null) return;
        cases.BeginUpdate();
        try {
            cases.Items.Clear();
            foreach(Map row in data) {
                var item=new ListViewItem(new[]{
                    Local(Bridge.Text(row,"updated")), Bridge.Text(row,"title"), Friendly(Bridge.Text(row,"state")),
                    Bridge.Text(row,"model_summary"), Bridge.Text(row,"member_count")+" / "+Bridge.Text(row,"attachment_count"),
                    Bridge.Text(row,"t3_thread_id")
                });
                item.Tag=row; cases.Items.Add(item);
                if(Bridge.Text(row,"id")==selectedCase) item.Selected=true;
            }
            if(cases.Items.Count>0 && cases.SelectedItems.Count==0) cases.Items[0].Selected=true;
            if(cases.Items.Count==0){ selectedCase=""; ++detailVersion; detail.Text="暂无业务事项。"; }
        } finally { cases.EndUpdate(); }
    }
    void FillJobs(object rows)
    {
        var data=rows as IEnumerable; if(data==null) return;
        jobs.BeginUpdate();
        try {
            jobs.Items.Clear();
            foreach(Map row in data) {
                string route=Bridge.Text(row,"route_kind")+" / "+Bridge.Text(row,"requested_model")+" "+Bridge.Text(row,"requested_effort");
                string slot=Bridge.Text(row,"execution_slot");
                if(slot.Length>0) route=Friendly(slot)+" · "+route;
                var item=new ListViewItem(new[]{ Local(Bridge.Text(row,"created")), Friendly(Bridge.Text(row,"state")), route.Trim(), Bridge.Text(row,"thread_id") });
                item.Tag=row; jobs.Items.Add(item);
            }
        } finally { jobs.EndUpdate(); }
    }
    void FillConclusions(Map data)
    {
        if(data==null) return;
        notesList.BeginUpdate();
        try {
            notesList.Items.Clear();
            var notes=Bridge.Get(data,"notes") as IEnumerable;
            if(notes!=null) foreach(Map n in notes) {
                string ch=Bridge.Text(n,"chat"); if(ch.Length>8) ch="…"+ch.Substring(ch.Length-8);
                notesList.Items.Add(new ListViewItem(new[]{ Bridge.Text(n,"note_id"), Bridge.Text(n,"title"), ch, UnixLocal(Bridge.Get(n,"updated_at")) }));
            }
        } finally { notesList.EndUpdate(); }
        followupsList.BeginUpdate();
        try {
            followupsList.Items.Clear();
            var fups=Bridge.Get(data,"followups") as IEnumerable;
            if(fups!=null) foreach(Map f in fups) {
                string id=Bridge.Text(f,"id"); string shortId=id.Length>6 ? id.Substring(id.Length-6) : id;
                string ch=Bridge.Text(f,"chat"); if(ch.Length>8) ch="…"+ch.Substring(ch.Length-8);
                string st=Bridge.Text(f,"status")=="open" ? "待人工补充" : "已处理";
                var item=new ListViewItem(new[]{ shortId, st, Bridge.Text(f,"question"), ch });
                item.Tag=id; followupsList.Items.Add(item);
            }
        } finally { followupsList.EndUpdate(); }
    }
    static void Fill(ListView table, object rows, params string[] fields)
    {
        var data=rows as IEnumerable; if(data==null) return;
        int first=table.TopItem==null ? 0 : table.TopItem.Index;
        table.BeginUpdate();
        try {
            table.Items.Clear();
            foreach(Map row in data) {
                var cells=Array.ConvertAll(fields,k=>Bridge.Text(row,k));
                cells[0]=Local(cells[0]); cells[1]=Friendly(cells[1]);
                if(fields[2]=="detail") cells[2]=Friendly(cells[2]);
                table.Items.Add(new ListViewItem(cells));
            }
            if(first>0 && first<table.Items.Count) table.TopItem=table.Items[first];
        } finally { table.EndUpdate(); }
    }
    internal async void RefreshData()
    {
        if(busy) return; busy=true;
        try {
            var data=await Bridge.Call("snapshot",null); if(IsDisposed) return;
            Fill(history,Bridge.Get(data,"log"),"at","kind","detail","msgid");
            FillJobs(Bridge.Get(data,"jobs"));
            FillCases(Bridge.Get(data,"cases"));
            FillConclusions(Bridge.Get(data,"conclusions") as Map);
            SyncModels(Bridge.Get(data,"settings") as Map,Bridge.Get(data,"model_catalog"));
            var counts=Bridge.Get(data,"case_counts") as Map;
            if(!host.Starting)
                feedback.Text="重槽 "+Bridge.Text(data,"regular_slots")+" / 轻槽 "+Bridge.Text(data,"light_slots")+"　待派发 "+Bridge.Text(counts,"queued")+"　执行中 "+Bridge.Text(counts,"running")+"　待确认/待补充 "+Bridge.Text(counts,"clarifying")+"　需处理 "+Bridge.Text(counts,"attention")+"　已完成 "+Bridge.Text(counts,"completed");
        } catch(Exception e){ feedback.Text="读取失败 · "+e.Message; }
        finally { busy=false; }
    }
}
